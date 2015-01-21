package catena

import (
	"sync"
)

// memoryPartition represents a catena partition
// stored in memory.
type memoryPartition struct {
	// Metadata
	minTS int64
	maxTS int64
	ro    bool

	sources []*memorySource

	walLock sync.Mutex
	// Write-ahead log
	log wal

	lock sync.RWMutex
}

// memorySource is a metric source registered in memory.
type memorySource struct {
	name string

	lock    sync.RWMutex
	metrics []*memoryMetric
}

// memoryMetric is a metric in memory.
type memoryMetric struct {
	name string

	lock   sync.RWMutex
	points []Point
}

// newMemoryPartition creates a new memoryPartition.
func newMemoryPartition(log wal) (*memoryPartition, error) {
	p := &memoryPartition{
		log: log,
	}

	// Load data from the WAL.
	for entry, err := log.readEntry(); err == nil; entry, err = log.readEntry() {
		if entry.operation == operationInsert {
			sources := map[string]map[string][]Point{}

			for _, row := range entry.rows {
				metrics := sources[row.Source]

				if metrics == nil {
					metrics = map[string][]Point{}
				}

				metrics[row.Metric] = append(metrics[row.Metric],
					Point{
						Timestamp: row.Timestamp,
						Value:     row.Value,
					})

				sources[row.Source] = metrics
			}

			for source, metrics := range sources {
				for metric, points := range metrics {
					p.addPoints(source, metric, points)
				}
			}
		}
	}

	// Truncate WAL.
	err := log.truncate()
	if err != nil {
		return nil, err
	}

	return p, nil
}

// minTimestamp returns the minimum timestamp
// value held by this partition.
func (p *memoryPartition) minTimestamp() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.minTS
}

// maxTimestamp returns the maximum timestamp
// value held by this partition.
func (p *memoryPartition) maxTimestamp() int64 {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.maxTS
}

// readOnly returns whether this partition is read only.
func (p *memoryPartition) readOnly() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.ro
}

// setReadOnly sets the the readOnly flag.
func (p *memoryPartition) setReadOnly() {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.ro = true
}

// filename returns the filename of the WAL used
// by this partition.
func (p *memoryPartition) filename() string {
	return p.log.filename()
}

// put adds rows to a partition.
func (p *memoryPartition) put(rows Rows) error {
	p.lock.Lock()

	if p.ro {
		p.lock.Unlock()
		return errorReadyOnlyPartition
	}

	_, err := p.log.append(walEntry{
		operation: operationInsert,
		rows:      rows,
	})

	if err != nil {
		// Something went wrong writing to the WAL.
		// Truncate to prepare for future writes.
		_ = p.log.truncate()
		// TODO: What happens if we can't truncate?!

		p.lock.Unlock()
		return err
	}

	p.lock.Unlock()

	sources := map[string]map[string][]Point{}

	for _, row := range rows {
		metrics := sources[row.Source]

		if metrics == nil {
			metrics = map[string][]Point{}
		}

		metrics[row.Metric] = append(metrics[row.Metric],
			Point{
				Timestamp: row.Timestamp,
				Value:     row.Value,
			})

		sources[row.Source] = metrics
	}

	for source, metrics := range sources {
		for metric, points := range metrics {
			p.addPoints(source, metric, points)
		}
	}

	// Writes don't return errors.
	return nil
}

// addPoints adds points to the partition.
func (p *memoryPartition) addPoints(source, metric string, points []Point) {
	p.lock.Lock()
	if len(p.sources) == 0 {
		// This is the first point.
		if len(points) > 0 {
			p.minTS = points[0].Timestamp
			p.maxTS = points[0].Timestamp
		}
	}

	// What follow are in-order insertions
	// into the sources, metrics, and points
	// slices.

	sourceIndex := -1
	metricIndex := -1

	insertionPoint := -1

	// TODO: use a binary search
	for i, src := range p.sources {
		if src.name == source {
			sourceIndex = i
			break
		}

		if src.name > source {
			insertionPoint = i
			break
		}
	}

	var src *memorySource

	if sourceIndex == -1 {
		// memorySource was not found.
		src = &memorySource{}
		src.name = source

		if insertionPoint == -1 {
			// Insert at the end.
			insertionPoint = len(p.sources)
		}

		// Update the sources slice.
		p.sources = append(p.sources[:insertionPoint],
			append([]*memorySource{src},
				p.sources[insertionPoint:]...,
			)...,
		)

		sourceIndex = insertionPoint
	}

	src = p.sources[sourceIndex]

	p.lock.Unlock()
	src.lock.Lock()

	// Now we do essentially the same
	// thing for the metric.
	insertionPoint = -1

	for i, met := range src.metrics {
		if met.name == metric {
			metricIndex = i
			break
		}

		if met.name > metric {
			insertionPoint = i
			break
		}
	}

	var met *memoryMetric

	if metricIndex == -1 {
		// memoryMetric was not found.
		met = &memoryMetric{}
		met.name = metric

		if insertionPoint == -1 {
			// Insert at the end.
			insertionPoint = len(src.metrics)
		}

		// Update the metrics slice.
		src.metrics = append(src.metrics[:insertionPoint],
			append([]*memoryMetric{met},
				src.metrics[insertionPoint:]...,
			)...,
		)

		metricIndex = insertionPoint
	}

	met = src.metrics[metricIndex]

	src.lock.Unlock()

	// Insert the points in order.

NEXT_POINT:
	for _, newPoint := range points {
		timestamp := newPoint.Timestamp
		value := newPoint.Value

		switch {
		case timestamp < p.minTS:
			p.minTS = timestamp
		case timestamp > p.maxTS:
			p.maxTS = timestamp
		}

		insertionPoint = -1

		met.lock.Lock()

		// We start from the end because the expected
		// case is appending to the end.
		lastIndex := len(met.points) - 1
		for i := lastIndex; i >= 0; i-- {

			ts := met.points[i].Timestamp

			switch {
			case ts == timestamp:
				met.points[i].Value = value
				continue NEXT_POINT

			case ts > timestamp:
				insertionPoint = i

			default:
				break
			}

		}

		if insertionPoint == -1 {
			insertionPoint = lastIndex + 1
		}

		// Update the points slice.
		met.points = append(met.points[:insertionPoint],
			append([]Point{newPoint},
				met.points[insertionPoint:]...,
			)...,
		)

		met.lock.Unlock()
	}
}

// fetchPoints returns an ordered slice of points for the
// (source, metric) series within the [start, end] time range.
func (p *memoryPartition) fetchPoints(source, metric string,
	start, end int64) ([]Point, error) {
	p.lock.RLock()

	if len(p.sources) == 0 {
		p.lock.RUnlock()
		return nil, errorSourceNotFound
	}

	p.lock.RUnlock()

	points := []Point{}

	timeRange := end - start

	// Iterate through sources
	p.lock.RLock()
	for _, src := range p.sources {
		if src.name == source {
			p.lock.RUnlock()
			// Iterate through metrics

			src.lock.RLock()
			for _, met := range src.metrics {
				if met.name == metric {
					src.lock.RUnlock()

					met.lock.RLock()
					// Iterate through points
					for _, point := range met.points {
						if point.Timestamp-start < 0 {
							continue
						}

						if point.Timestamp-start <= timeRange {
							points = append(points, point)
						}

						if point.Timestamp > end {
							break
						}
					}

					met.lock.RUnlock()
					break
				}

				if met.name > metric {
					src.lock.RUnlock()
					return nil, errorMetricNotFound
				}
			}
			break
		}

		if src.name > source {
			p.lock.RUnlock()
			return nil, errorSourceNotFound
		}
	}

	return points, nil
}

func (p *memoryPartition) close() error {
	p.setReadOnly()

	p.lock.Lock()
	p.sources = nil
	p.lock.Unlock()

	return p.log.close()
}

func (p *memoryPartition) destroy() error {
	err := p.close()
	if err != nil {
		logger.Println(err)
		return err
	}

	return p.log.destroy()
}

// memoryPartition is a partition
var _ partition = &memoryPartition{}
