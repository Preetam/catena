// Package catena provides a storage engine for
// time series data.
package catena

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

var (
	maxPartitions    = 2 * 30    // 30 days
	partitionModulus = 3600 * 12 // 12 hours
)

// DB is a handle to a catena database.
type DB struct {

	// Data directory
	baseDir string

	partitionsLock sync.RWMutex
	partitions     []partition

	lastPartitionID  int
	partitionModulus int
	maxPartitions    int
}

// NewDB creates a new DB located in baseDir. If baseDir
// does not exist it will be created. An error is returned
// if baseDir is not empty.
func NewDB(baseDir string) (*DB, error) {
	err := os.MkdirAll(baseDir, 0755)
	if err != nil {
		return nil, err
	}

	dir, err := os.Open(baseDir)
	if err != nil {
		return nil, err
	}

	defer dir.Close()

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	if len(names) > 0 {
		return nil, errors.New("catena: NewDB called with non-empty directory")
	}

	return &DB{
		baseDir:          baseDir,
		partitionModulus: partitionModulus,
		maxPartitions:    maxPartitions,
	}, nil
}

// OpenDB opens a DB located in baseDir.
func OpenDB(baseDir string) (*DB, error) {
	db := &DB{
		baseDir:          baseDir,
		partitionModulus: partitionModulus,
		maxPartitions:    maxPartitions,
	}

	dir, err := os.Open(baseDir)
	if err != nil {
		return nil, err
	}

	defer dir.Close()

	dirInfo, err := dir.Stat()
	if err != nil {
		return nil, err
	}

	if !dirInfo.IsDir() {
		return nil, errors.New("catena: baseDir is not a directory")
	}

	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	err = db.loadPartitions(names)
	if err != nil {
		return nil, err
	}

	return db, nil
}

// loadPartitions reads a slice of partition file names
// and updates the internal partition state.
func (db *DB) loadPartitions(names []string) error {

	// Slice of partition IDs
	partitions := []int{}

	isWAL := map[int]bool{}

	// TODO:
	// Make sure we don't add duplicates. If there
	// is a crash during a WAL compaction, we will
	// see a #.wal and a #.part file, and the latter
	// will not be valid. In this case, we need to
	// restart the compaction.

	for _, name := range names {
		if name == logFileName {
			continue
		}

		partitionNum := -1

		if strings.HasSuffix(name, ".wal") {
			_, err := fmt.Sscanf(name, "%d.wal", &partitionNum)
			if err != nil {
				return err
			}

			partitions = append(partitions, partitionNum)
			isWAL[partitionNum] = true

			continue
		}

		_, err := fmt.Sscanf(name, "%d.part", &partitionNum)
		if err != nil {
			return err
		}

		if partitionNum < 0 {
			return errors.New("catena: invalid partition number")
		}

		partitions = append(partitions, partitionNum)
	}

	// Sort the partitions in increasing order.
	sort.Ints(partitions)

	for _, part := range partitions {
		if part > db.lastPartitionID {
			db.lastPartitionID = part
		}

		if isWAL[part] {
			filename := filepath.Join(db.baseDir,
				fmt.Sprintf("%d.wal", part))

			log, err := openFileWAL(filename)
			if err != nil {
				return err
			}

			mp, err := newMemoryPartition(log)
			if err != nil {
				return err
			}

			db.partitions = append(db.partitions, mp)
			continue
		}
		filename := filepath.Join(db.baseDir,
			fmt.Sprintf("%d.part", part))

		fp, err := openFilePartition(filename)
		if err != nil {
			return err
		}

		db.partitions = append(db.partitions, fp)
	}

	return nil
}
