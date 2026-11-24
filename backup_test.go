package starskey

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func backupTestConfig(directory string, bloomFilter, surf bool) *Config {
	return &Config{
		Permission:     0o755,
		Directory:      directory,
		FlushThreshold: 1024 * 1024,
		MaxLevel:       3,
		SizeFactor:     4,
		BloomFilter:    bloomFilter,
		SuRF:           surf,
		Logging:        false,
	}
}

func openBackupTestDB(t *testing.T, directory string, bloomFilter, surf bool) *Starskey {
	t.Helper()

	db, err := Open(backupTestConfig(directory, bloomFilter, surf))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}

	t.Cleanup(func() {
		_ = db.Close()
	})

	return db
}

func putBackupTestRecords(t *testing.T, db *Starskey, prefix string, count int, valueSize int) {
	t.Helper()

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("%s-%03d", prefix, i))
		value := bytes.Repeat([]byte{byte('a' + i%26)}, valueSize)
		if err := db.Put(key, value); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
}

func assertBackupTestRecords(t *testing.T, db *Starskey, prefix string, count int, valueSize int) {
	t.Helper()

	for i := 0; i < count; i++ {
		key := []byte(fmt.Sprintf("%s-%03d", prefix, i))
		expected := bytes.Repeat([]byte{byte('a' + i%26)}, valueSize)

		value, err := db.Get(key)
		if err != nil {
			t.Fatalf("get %s from restored backup: %v", key, err)
		}
		if !bytes.Equal(value, expected) {
			t.Fatalf("key %s mismatch: got %q, want %q", key, value, expected)
		}
	}
}

func TestStarskey_BackupDuringWALRotationRestoresConsistentSnapshot(t *testing.T) {
	filterModes := []struct {
		name        string
		bloomFilter bool
		surf        bool
	}{
		{name: "none"},
		{name: "bloom", bloomFilter: true},
		{name: "surf", surf: true},
	}

	for _, filterMode := range filterModes {
		t.Run(filterMode.name, func(t *testing.T) {
			root := t.TempDir()
			databaseDirectory := filepath.Join(root, "db")
			backupDirectory := filepath.Join(root, "backup")
			restoreDirectory := filepath.Join(root, "restore")

			db := openBackupTestDB(t, databaseDirectory, filterMode.bloomFilter, filterMode.surf)

			const sstRecordCount = 24
			const walRecordCount = 16
			const valueSize = 96

			putBackupTestRecords(t, db, "sst", sstRecordCount, valueSize)

			db.lock.Lock()
			if err := db.run(); err != nil {
				db.lock.Unlock()
				t.Fatalf("flush first SSTable: %v", err)
			}
			db.lock.Unlock()

			putBackupTestRecords(t, db, "wal", walRecordCount, valueSize)

			rotated := false
			err := db.backup(backupDirectory, backupOptions{
				afterSnapshot: func() error {
					writeCompleted := make(chan error, 1)
					go func() {
						writeCompleted <- db.Put([]byte("concurrent-write"), []byte("progress"))
					}()

					select {
					case err := <-writeCompleted:
						if err != nil {
							return err
						}
					case <-time.After(5 * time.Second):
						return fmt.Errorf("online write blocked while backup copied data")
					}

					db.lock.Lock()
					defer db.lock.Unlock()

					if db.wal.PageCount() == 0 {
						return fmt.Errorf("WAL disappeared before rotation")
					}

					if err := db.run(); err != nil {
						return fmt.Errorf("rotate WAL: %w", err)
					}

					if db.wal.Size() != 0 {
						return fmt.Errorf("WAL was not truncated by rotation: size=%d", db.wal.Size())
					}
					if len(db.levels) > 0 && len(db.levels[0].sstables) >= 2 {
						if err := db.compact(0); err != nil {
							return fmt.Errorf("compact after rotation: %w", err)
						}
					}

					rotated = true
					t.Logf("rotated WAL and compacted while backup snapshot was active")
					return nil
				},
			})
			if err != nil {
				t.Fatalf("backup during WAL rotation: %v", err)
			}
			if !rotated {
				t.Fatal("rotation hook did not run")
			}

			// This write happens after the point-in-time snapshot and must not
			// appear in the restored backup.
			if err := db.Put([]byte("after-backup"), []byte("not-in-backup")); err != nil {
				t.Fatalf("put after backup: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatalf("close source database: %v", err)
			}

			backupWAL, err := os.Stat(filepath.Join(backupDirectory, WALExtension))
			if err != nil {
				t.Fatalf("backup WAL: %v", err)
			}
			if backupWAL.Size() == 0 {
				t.Fatal("backup WAL is empty; records can be lost when source WAL rotates")
			}
			t.Logf("backup retained %d WAL bytes from the aligned snapshot", backupWAL.Size())

			if err := RestoreBackup(backupDirectory, restoreDirectory); err != nil {
				t.Fatalf("restore backup: %v", err)
			}

			restored := openBackupTestDB(t, restoreDirectory, filterMode.bloomFilter, filterMode.surf)
			assertBackupTestRecords(t, restored, "sst", sstRecordCount, valueSize)
			assertBackupTestRecords(t, restored, "wal", walRecordCount, valueSize)

			value, err := restored.Get([]byte("after-backup"))
			if err != nil {
				t.Fatalf("get post-snapshot key: %v", err)
			}
			if value != nil {
				t.Fatalf("post-snapshot write leaked into backup: %q", value)
			}

			value, err = restored.Get([]byte("concurrent-write"))
			if err != nil {
				t.Fatalf("get concurrent post-snapshot key: %v", err)
			}
			if value != nil {
				t.Fatalf("concurrent write after snapshot leaked into backup: %q", value)
			}

			if err := restored.Close(); err != nil {
				t.Fatalf("close restored database: %v", err)
			}

			reopened := openBackupTestDB(t, restoreDirectory, filterMode.bloomFilter, filterMode.surf)
			assertBackupTestRecords(t, reopened, "sst", sstRecordCount, valueSize)
			assertBackupTestRecords(t, reopened, "wal", walRecordCount, valueSize)
		})
	}
}

func TestStarskey_BackupCopiesConsistentSnapshot(t *testing.T) {
	root := t.TempDir()
	databaseDirectory := filepath.Join(root, "db")
	backupDirectory := filepath.Join(root, "backup")
	restoreDirectory := filepath.Join(root, "restore")

	db := openBackupTestDB(t, databaseDirectory, false, false)
	putBackupTestRecords(t, db, "key", 40, 64)

	if err := db.Backup(backupDirectory); err != nil {
		t.Fatalf("backup: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	if err := RestoreBackup(backupDirectory, restoreDirectory); err != nil {
		t.Fatalf("restore: %v", err)
	}

	restored := openBackupTestDB(t, restoreDirectory, false, false)
	assertBackupTestRecords(t, restored, "key", 40, 64)
}

func TestRestoreBackupRejectsNonEmptyDestination(t *testing.T) {
	root := t.TempDir()
	backupDirectory := filepath.Join(root, "backup")
	destinationDirectory := filepath.Join(root, "restore")

	db := openBackupTestDB(t, filepath.Join(root, "db"), false, false)
	if err := db.Backup(backupDirectory); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}

	if err := os.MkdirAll(destinationDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(destinationDirectory, "stale"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := RestoreBackup(backupDirectory, destinationDirectory); err == nil {
		t.Fatal("expected restore into non-empty directory to fail")
	}
}

func TestStarskey_BackupWithConcurrentWrites(t *testing.T) {
	root := t.TempDir()
	databaseDirectory := filepath.Join(root, "db")
	config := backupTestConfig(databaseDirectory, false, false)
	config.FlushThreshold = 2048

	db, err := Open(config)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	stableValue := []byte("stable-value")
	if err := db.Put([]byte("stable-key"), stableValue); err != nil {
		t.Fatalf("put stable key: %v", err)
	}

	stopWrites := make(chan struct{})
	var writerWG sync.WaitGroup
	var writeErr error
	var writeErrMu sync.Mutex

	writerWG.Add(1)
	go func() {
		defer writerWG.Done()

		for i := 0; ; i++ {
			select {
			case <-stopWrites:
				return
			default:
			}

			key := []byte(fmt.Sprintf("concurrent-key-%06d", i))
			value := bytes.Repeat([]byte("x"), 128)
			if err := db.Put(key, value); err != nil {
				writeErrMu.Lock()
				writeErr = err
				writeErrMu.Unlock()
				return
			}
		}
	}()

	time.Sleep(25 * time.Millisecond)

	for i := 0; i < 3; i++ {
		backupDirectory := filepath.Join(root, fmt.Sprintf("backup-%d", i))
		restoreDirectory := filepath.Join(root, fmt.Sprintf("restore-%d", i))

		if err := db.Backup(backupDirectory); err != nil {
			close(stopWrites)
			writerWG.Wait()
			t.Fatalf("concurrent backup %d: %v", i, err)
		}

		if err := RestoreBackup(backupDirectory, restoreDirectory); err != nil {
			close(stopWrites)
			writerWG.Wait()
			t.Fatalf("restore concurrent backup %d: %v", i, err)
		}

		restored := openBackupTestDB(t, restoreDirectory, false, false)
		value, err := restored.Get([]byte("stable-key"))
		if err != nil {
			t.Fatalf("get stable key from backup %d: %v", i, err)
		}
		if !bytes.Equal(value, stableValue) {
			t.Fatalf("backup %d stable key mismatch: %q", i, value)
		}
		if err := restored.Close(); err != nil {
			t.Fatalf("close restored backup %d: %v", i, err)
		}
	}

	close(stopWrites)
	writerWG.Wait()

	writeErrMu.Lock()
	err = writeErr
	writeErrMu.Unlock()
	if err != nil {
		t.Fatalf("concurrent writer failed: %v", err)
	}
}
