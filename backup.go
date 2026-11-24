package starskey

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type backupOptions struct {
	afterSnapshot func() error
}

// Backup creates a copy of the database that can be opened directly or restored
// with RestoreBackup.
//
// Starskey holds its write lock only while establishing the point-in-time
// snapshot. Large, immutable SST files are linked into a staging directory and
// copied after the lock is released. The WAL is copied byte-for-byte while the
// lock is held, so a truncation/rotation immediately after the snapshot cannot
// remove records needed by the backup.
func (skey *Starskey) Backup(destination string) error {
	return skey.backup(destination, backupOptions{})
}

func (skey *Starskey) backup(destination string, options backupOptions) error {
	if skey == nil || skey.config == nil || skey.lock == nil || skey.wal == nil {
		return errors.New("starskey is not open")
	}
	if destination == "" {
		return errors.New("backup destination cannot be empty")
	}

	sourceDirectory := filepath.Clean(skey.config.Directory)
	destinationDirectory := filepath.Clean(destination)

	stageDirectory, err := os.MkdirTemp(sourceDirectory, ".backup-stage-*")
	if err != nil {
		return fmt.Errorf("create backup staging directory: %w", err)
	}
	defer func() {
		_ = os.RemoveAll(stageDirectory)
	}()

	if err := os.Chmod(stageDirectory, skey.config.Permission); err != nil {
		return fmt.Errorf("set backup staging permissions: %w", err)
	}

	skey.lock.Lock()
	err = skey.snapshotForBackup(stageDirectory)
	skey.lock.Unlock()
	if err != nil {
		return err
	}

	if options.afterSnapshot != nil {
		if err := options.afterSnapshot(); err != nil {
			return err
		}
	}

	if err := copyTree(stageDirectory, destinationDirectory); err != nil {
		return err
	}

	return syncDirectory(destinationDirectory)
}

func (skey *Starskey) snapshotForBackup(stageDirectory string) error {
	// Make sure the current WAL contents are visible before copying them.
	skey.wal.EscalateFSync()

	walPath := skey.wal.Name()
	walDestination := filepath.Join(stageDirectory, filepath.Base(walPath))
	if err := copyFile(walPath, walDestination); err != nil {
		return fmt.Errorf("snapshot WAL: %w", err)
	}

	sourceDirectory := filepath.Clean(skey.config.Directory)

	for level := uint64(1); level <= skey.config.MaxLevel; level++ {
		levelName := fmt.Sprintf("%s%d", LevelPrefix, level)
		sourceLevel := filepath.Join(sourceDirectory, levelName)
		stageLevel := filepath.Join(stageDirectory, levelName)

		info, err := os.Stat(sourceLevel)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("stat level %d: %w", level, err)
		}

		if err := os.MkdirAll(stageLevel, info.Mode().Perm()); err != nil {
			return fmt.Errorf("create snapshot level %d: %w", level, err)
		}

		entries, err := os.ReadDir(sourceLevel)
		if err != nil {
			return fmt.Errorf("read level %d: %w", level, err)
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			sourcePath := filepath.Join(sourceLevel, entry.Name())
			stagePath := filepath.Join(stageLevel, entry.Name())

			// K logs and value logs are immutable once published. A hard link
			// keeps their inode alive if compaction removes the source name
			// while the backup copy is still in progress.
			switch filepath.Ext(entry.Name()) {
			case KLogExtension, VLogExtension:
				if err := os.Link(sourcePath, stagePath); err == nil {
					continue
				}
			}

			// Filters and any small sidecar files are copied while the write
			// lock is held. Some filter files can be rewritten in place.
			if err := copyFile(sourcePath, stagePath); err != nil {
				return fmt.Errorf("snapshot %s: %w", entry.Name(), err)
			}
		}
	}

	return nil
}

// RestoreBackup copies a backup created by Backup into destinationDirectory.
// The destination must not already contain files, preventing an old SSTable from
// being mixed into a restored snapshot.
func RestoreBackup(backupDirectory, destinationDirectory string) error {
	if backupDirectory == "" || destinationDirectory == "" {
		return errors.New("backup and destination directories cannot be empty")
	}

	backupDirectory = filepath.Clean(backupDirectory)
	destinationDirectory = filepath.Clean(destinationDirectory)
	if pathWithin(backupDirectory, destinationDirectory) {
		return errors.New("restore destination must not be inside the backup directory")
	}

	walPath := filepath.Join(backupDirectory, WALExtension)
	if _, err := os.Stat(walPath); err != nil {
		return fmt.Errorf("invalid starskey backup %q: %w", backupDirectory, err)
	}

	if err := copyTree(backupDirectory, destinationDirectory); err != nil {
		return err
	}

	return syncDirectory(destinationDirectory)
}

func pathWithin(parent, child string) bool {
	relative, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	if relative == "." {
		return true
	}
	if relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return false
	}
	return !filepath.IsAbs(relative)
}

func copyTree(source, destination string) error {
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", source)
	}

	if err := os.MkdirAll(destination, info.Mode().Perm()); err != nil {
		return err
	}

	entries, err := os.ReadDir(destination)
	if err != nil {
		return err
	}
	if len(entries) != 0 {
		return fmt.Errorf("destination %q is not empty", destination)
	}

	return copyTreeContents(source, destination, info.Mode().Perm())
}

func copyTreeContents(source, destination string, dirMode os.FileMode) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		sourcePath := filepath.Join(source, entry.Name())
		destinationPath := filepath.Join(destination, entry.Name())

		info, err := entry.Info()
		if err != nil {
			return err
		}

		switch {
		case entry.IsDir():
			if err := os.MkdirAll(destinationPath, info.Mode().Perm()); err != nil {
				return err
			}
			if err := copyTreeContents(sourcePath, destinationPath, info.Mode().Perm()); err != nil {
				return err
			}
		case info.Mode()&os.ModeSymlink != 0:
			linkTarget, err := os.Readlink(sourcePath)
			if err != nil {
				return err
			}
			if err := os.Symlink(linkTarget, destinationPath); err != nil {
				return err
			}
		case entry.Type().IsRegular():
			if err := copyFile(sourcePath, destinationPath); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported file type for %s", sourcePath)
		}
	}

	return os.Chmod(destination, dirMode)
}

func copyFile(source, destination string) (err error) {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer func() {
		_ = input.Close()
	}()

	info, err := input.Stat()
	if err != nil {
		return err
	}

	output, err := os.OpenFile(destination, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	defer func() {
		closeErr := output.Close()
		if err == nil {
			err = closeErr
		}
	}()

	if _, err := io.Copy(output, input); err != nil {
		return err
	}

	if err := output.Sync(); err != nil {
		return err
	}

	return os.Chmod(destination, info.Mode().Perm())
}

func syncDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer func() {
		_ = handle.Close()
	}()

	return handle.Sync()
}
