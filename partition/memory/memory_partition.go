package memory

import (
	"errors"
	"io"
	"sync"

	"github.com/PreetamJinka/catena"
	"github.com/PreetamJinka/catena/wal"
)

// MemoryPartition is a partition that exists in-memory.
type MemoryPartition struct {
	readOnly      bool
	partitionLock sync.RWMutex

	minTS int64
	maxTS int64

	sources     map[string]*memorySource
	sourcesLock sync.Mutex

	wal wal.WAL
}

// memorySource is a source with metrics.
type memorySource struct {
	name    string
	metrics map[string]*memoryMetric
	lock    sync.Mutex
}

// memoryMetric contains an ordered slice of points.
type memoryMetric struct {
	name   string
	points []catena.Point
	lock   sync.Mutex

	lastInsertIndex int
}

// NewMemoryPartition creates a new MemoryPartition backed by WAL.
func NewMemoryPartition(WAL wal.WAL) *MemoryPartition {
	p := MemoryPartition{
		readOnly: false,
		sources:  map[string]*memorySource{},
		wal:      WAL,
	}

	return &p
}

// RecoverMemoryPartition recovers a MemoryPartition backed by WAL.
func RecoverMemoryPartition(WAL wal.WAL) (*MemoryPartition, error) {
	p := &MemoryPartition{
		readOnly: false,
		sources:  map[string]*memorySource{},
	}

	var entry wal.WALEntry
	var err error

	for entry, err = WAL.ReadEntry(); err == nil; entry, err = WAL.ReadEntry() {
		p.InsertRows(entry.Rows)
	}

	if err != nil {
		if err != io.EOF {
			return nil, err
		}
	}

	err = WAL.Truncate()

	p.wal = WAL

	return p, err
}

// InsertRows inserts rows into the partition.
func (p *MemoryPartition) InsertRows(rows []catena.Row) error {
	p.partitionLock.RLock()
	if p.readOnly {
		p.partitionLock.RUnlock()
		return errors.New("partition/memory: read only")
	}

	if p.wal != nil {
		_, err := p.wal.Append(wal.WALEntry{
			Operation: wal.OperationInsert,
			Rows:      rows,
		})

		if err != nil {
			p.partitionLock.RUnlock()
			return err
		}
	}
	p.partitionLock.RUnlock()

	var (
		minTS int64
		maxTS int64
	)

	for _, row := range rows {

		if minTS == maxTS && minTS == 0 {
			minTS = row.Timestamp
			maxTS = row.Timestamp
		}

		if row.Timestamp < minTS {
			minTS = row.Timestamp
		}

		if row.Timestamp > maxTS {
			maxTS = row.Timestamp
		}

		source := p.getOrCreateSource(row.Source)
		metric := source.getOrCreateMetric(row.Metric)
		metric.insertPoints([]catena.Point{row.Point})
	}

	p.partitionLock.Lock()
	if minTS < p.minTS {
		p.minTS = minTS
	}

	if maxTS > p.maxTS {
		p.maxTS = maxTS
	}
	p.partitionLock.Unlock()

	return nil
}

// Destroy destroys the memory partition as well as its WAL.
func (m *MemoryPartition) Destroy() error {
	m.partitionLock.Lock()

	// Destroy WAL
	err := m.wal.Destroy()

	m.readOnly = true
	m.partitionLock.Unlock()

	return err
}