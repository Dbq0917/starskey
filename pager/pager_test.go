// Package pager
//
// (C) Copyright Starskey
//
// Original Author: Alex Gaetano Padula
//
// Licensed under the Mozilla Public License, v. 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// https://www.mozilla.org/en-US/MPL/2.0/
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package pager

import (
	"log"
	"os"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 1024, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}

	// Close
	if err := p.Close(); err != nil {
		t.Errorf("Error closing file: %v", err)
	}
}

func TestChunk(t *testing.T) {
	data := []byte("hello world")
	chunks, err := chunk(data, 5)
	if err != nil {
		t.Errorf("Error chunking data: %v", err)

	}
	for i, c := range chunks {
		t.Logf("Chunk %d: %s", i, string(c))

	}
	if len(chunks) != 4 {
		t.Errorf("Expected 2 chunks, got %d", len(chunks))
	}
}

func TestPager_Write(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 4, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}

	defer p.Close()

	pg, err := p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	// Expect page 0 initially
	if pg != 0 {
		t.Errorf("Expected page 0, got %d", pg)
	}

	pg, err = p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	// Expect page 4
	if pg != 4 {
		t.Errorf("Expected page 0, got %d", pg)
	}
}

func TestPager_Read(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 4, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}

	defer p.Close()

	pg, err := p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	log.Println(pg)

	pg, err = p.Write([]byte("hello world2"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	log.Println(pg)
	pg, err = p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	log.Println(pg)

	data, _, err := p.Read(4)
	if err != nil {
		t.Errorf("Error reading from file: %v", err)
	}

	if string(data) != "hello world2" {
		t.Errorf("Expected 'hello world2', got %s", string(data))
	}
}

func TestPagerIterator(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 4, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}

	defer p.Close()

	p.Write([]byte("hello world"))
	p.Write([]byte("hello world2"))
	p.Write([]byte("hello world3"))

	it := NewIterator(p)
	for it.Next() {
		data, err := it.Read()
		if err != nil {
			break
		}
		log.Println(string(data))

	}

	for {
		if !it.Prev() {
			break
		}

		data, err := it.Read()
		if err != nil {
			break
		}
		log.Println(string(data))
	}
}

func TestPager_Truncate(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 1024, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}
	defer p.Close()

	_, err = p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	err = p.Truncate()
	if err != nil {
		t.Errorf("Error truncating file: %v", err)
	}

	size := p.Size()
	if size != 0 {
		t.Errorf("Expected file size 0, got %d", size)
	}
}

func TestPager_Size(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 1024, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}
	defer p.Close()

	_, err = p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	size := p.Size()
	if size <= 0 {
		t.Errorf("Expected file size greater than 0, got %d", size)
	}
}

func TestPager_EscalateFSync(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 1024, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}
	defer p.Close()

	_, err = p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	p.EscalateFSync()
	// No direct way to verify fsync
}

func TestPager_PageCount(t *testing.T) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 4, true, time.Millisecond*128)
	if err != nil {
		t.Errorf("Error opening file: %v", err)
	}
	defer p.Close()

	_, err = p.Write([]byte("hello world"))
	if err != nil {
		t.Errorf("Error writing to file: %v", err)
	}

	pageCount := p.PageCount()
	if pageCount != 4 {
		t.Errorf("Expected 4 pages, got %d", pageCount)
	}
}

func TestPager_ScanRecordsDiscardsTornTail(t *testing.T) {
	filename := t.TempDir() + "/scan.bin"
	p, err := Open(filename, os.O_CREATE|os.O_RDWR, 0777, 8, false, 0)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}

	if _, err := p.Write([]byte("first")); err != nil {
		t.Fatalf("Error writing first record: %v", err)
	}
	if _, err := p.Write([]byte("second")); err != nil {
		t.Fatalf("Error writing second record: %v", err)
	}
	completeSize := p.Size()
	if err := p.Close(); err != nil {
		t.Fatalf("Error closing pager: %v", err)
	}

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		t.Fatalf("Error opening file for torn append: %v", err)
	}
	if _, err := f.Write([]byte("torn")); err != nil {
		t.Fatalf("Error appending torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Error closing torn append: %v", err)
	}

	p, err = Open(filename, os.O_RDWR, 0777, 8, false, 0)
	if err != nil {
		t.Fatalf("Error reopening file: %v", err)
	}
	defer p.Close()

	records, validEnd, err := p.ScanRecords()
	if err != nil {
		t.Fatalf("Error scanning records: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("Expected 2 complete records, got %d", len(records))
	}
	if validEnd != completeSize {
		t.Fatalf("Expected valid end %d, got %d", completeSize, validEnd)
	}
	if string(records[0].Data) != "first" || string(records[1].Data) != "second" {
		t.Fatalf("Unexpected scanned records: %q %q", records[0].Data, records[1].Data)
	}

	count, countValidEnd, err := p.ScanRecordCount()
	if err != nil {
		t.Fatalf("Error counting records: %v", err)
	}
	if count != 2 || countValidEnd != completeSize {
		t.Fatalf("Unexpected count result: %d records ending at %d", count, countValidEnd)
	}

	var batched []string
	if _, err := p.ReadRecordBatches(1, func(batch []ScannedRecord) error {
		for _, record := range batch {
			batched = append(batched, string(record.Data))
		}
		return nil
	}); err != nil {
		t.Fatalf("Error reading record batches: %v", err)
	}
	if len(batched) != 2 || batched[0] != "first" || batched[1] != "second" {
		t.Fatalf("Unexpected batched records: %q", batched)
	}

	if err := p.TruncateTo(validEnd); err != nil {
		t.Fatalf("Error truncating torn tail: %v", err)
	}
	if p.Size() != validEnd {
		t.Fatalf("Expected size %d after truncation, got %d", validEnd, p.Size())
	}
}

func TestPager_ScanRecordsDiscardsIncompleteOverflowChain(t *testing.T) {
	filename := t.TempDir() + "/overflow.bin"
	p, err := Open(filename, os.O_CREATE|os.O_RDWR, 0777, 4, false, 0)
	if err != nil {
		t.Fatalf("Error opening file: %v", err)
	}

	if _, err := p.Write([]byte("abcdefghij")); err != nil {
		t.Fatalf("Error writing overflowing record: %v", err)
	}
	slotSize := int64(p.GetPageSize() + 16)
	if err := p.Close(); err != nil {
		t.Fatalf("Error closing pager: %v", err)
	}

	// Keep two pages of a three-page record.
	if err := os.Truncate(filename, slotSize*2); err != nil {
		t.Fatalf("Error truncating overflow chain: %v", err)
	}

	p, err = Open(filename, os.O_RDWR, 0777, 4, false, 0)
	if err != nil {
		t.Fatalf("Error reopening file: %v", err)
	}
	defer p.Close()

	records, validEnd, err := p.ScanRecords()
	if err != nil {
		t.Fatalf("Error scanning records: %v", err)
	}
	if len(records) != 0 || validEnd != 0 {
		t.Fatalf("Expected incomplete overflow chain to be discarded, got %d records ending at %d", len(records), validEnd)
	}
}

func BenchmarkPager_Write(b *testing.B) {
	defer os.Remove("test.bin")
	p, err := Open("test.bin", os.O_CREATE|os.O_RDWR, 0777, 128, true, time.Millisecond*128)
	if err != nil {
		b.Fatalf("Error opening file: %v", err)
	}
	defer p.Close()

	data := []byte("hello world")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := p.Write(data); err != nil {
			b.Fatalf("Error writing to file: %v", err)
		}
	}
}
