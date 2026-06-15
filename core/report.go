package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gosuri/uilive"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

const (
	barChar = "∎"
)

type Stats struct {
	Largest  int
	Smallest int
	Average  int
	Count    int
	Total    int

	sizes       []int
	countLock   sync.RWMutex
	sizeToCount map[int]int
}

type Report interface {
	Results() chan<- []*mvccpb.KeyValue
	Run() <-chan string
	DynamicOutput()
	JSON() string
}

type SizeOf func(*mvccpb.KeyValue) int

type report struct {
	results     chan []*mvccpb.KeyValue
	stats       Stats
	bucketCount int
	sizeOf      SizeOf
	writer      *uilive.Writer
	processOver atomic.Value
	dynamicOnce sync.Once
	jsonMode    bool
}

func (r *report) Results() chan<- []*mvccpb.KeyValue { return r.results }

func (r *report) Run() <-chan string {
	donec := make(chan string, 1)
	if r.jsonMode {
		// JSON mode: only collect stats, no text output
		go func() {
			defer close(donec)
			r.processResults()
		}()
	} else {
		// Text mode: original behavior with dynamic terminal output
		r.writer.Start()
		go func() {
			defer close(donec)
			r.processResults()
			if r.stats.Count <= 0 {
				_, _ = fmt.Fprintln(r.writer, "empty data")
			}
			r.finalString()
			r.writer.Stop()
		}()
	}

	return donec
}

func (r *report) processResults() {
	for res := range r.results {
		r.processResult(res)
	}
	r.processOver.Store(true)
	time.Sleep(time.Millisecond * 100)
}

func (r *report) processResult(res []*mvccpb.KeyValue) {
	l := len(res)
	if l == 0 {
		return
	}
	r.stats.Count += l
	for _, kv := range res {
		s := r.sizeOf(kv)
		r.stats.Smallest = Min(r.stats.Smallest, s)
		r.stats.Largest = Max(r.stats.Largest, s)
		r.stats.Total += s
		r.stats.countLock.Lock()
		_, ok := r.stats.sizeToCount[s]
		if !ok {
			r.stats.sizes = append(r.stats.sizes, s)
		}
		r.stats.sizeToCount[s] += 1
		r.stats.countLock.Unlock()
	}
	r.stats.Average = r.stats.Total / r.stats.Count
}

func (r *report) String() string {
	var buffer bytes.Buffer

	buffer.WriteString("Summary:\n")
	buffer.WriteString(fmt.Sprintf("  Count:\t%d.\n", r.stats.Count))
	buffer.WriteString(fmt.Sprintf("  Total:\t%s.\n", ReadableSize(r.stats.Total)))
	buffer.WriteString(fmt.Sprintf("  Smallest:\t%s.\n", ReadableSize(r.stats.Smallest)))
	buffer.WriteString(fmt.Sprintf("  Largest:\t%s.\n", ReadableSize(r.stats.Largest)))
	buffer.WriteString(fmt.Sprintf("  Average:\t%s.\n", ReadableSize(r.stats.Average)))

	sort.Ints(r.stats.sizes)
	buffer.WriteString(r.histogram())
	r.stats.countLock.RLock()
	buffer.WriteString(PrintPercent(r.stats.sizes, r.stats.sizeToCount))
	r.stats.countLock.RUnlock()

	return buffer.String()
}

func (r *report) DynamicOutput() {
	r.dynamicOnce.Do(func() {
		go func() {
			for {
				if r.processOver.Load().(bool) {
					return
				}
				r.dynamicString()
				time.Sleep(time.Millisecond * 100)
			}
		}()
	})
}

func (r *report) dynamicString() {
	if r.stats.Count <= 0 {
		return
	}

	_, _ = fmt.Fprint(r.writer, r.String())
	_ = r.writer.Flush()
}

func (r *report) finalString() {
	if r.stats.Count <= 0 {
		return
	}

	_, _ = fmt.Fprint(r.writer.Bypass(), r.String())
}

func (r *report) histogram() string {
	buckets := make([]int, r.bucketCount+1)
	counts := make([]int, r.bucketCount+1)
	bs := (r.stats.Largest - r.stats.Smallest) / r.bucketCount
	for i := 0; i < r.bucketCount; i++ {
		buckets[i] = r.stats.Smallest + bs*i
	}
	buckets[r.bucketCount] = r.stats.Largest

	var bi int
	var max int
	for i := 0; i < len(r.stats.sizes); {
		s := r.stats.sizes[i]
		if s <= buckets[bi] {
			i++
			r.stats.countLock.RLock()
			counts[bi] += r.stats.sizeToCount[s]
			r.stats.countLock.RUnlock()
			if max < counts[bi] {
				max = counts[bi]
			}
		} else if bi < len(buckets)-1 {
			bi++
		}
	}
	var buffer bytes.Buffer
	buffer.WriteString("\nSize histogram:\n")
	for i := 0; i < len(buckets); i++ {
		var barLen int
		if max > 0 {
			barLen = counts[i] * 40 / max
		}
		buffer.WriteString(fmt.Sprintf("  %s [%d]\t|%v\n", ReadableSize(buckets[i]), counts[i], strings.Repeat(barChar, barLen)))
	}
	return buffer.String()
}

func NewReport(bc int, of SizeOf, jsonMode ...bool) Report {
	isJSON := false
	if len(jsonMode) > 0 && jsonMode[0] {
		isJSON = true
	}
	r := &report{
		results: make(chan []*mvccpb.KeyValue),
		stats: Stats{
			Largest:     -1,
			Smallest:    math.MaxInt32,
			sizeToCount: make(map[int]int),
		},
		bucketCount: bc,
		sizeOf:      of,
		writer:      uilive.New(),
		jsonMode:    isJSON,
	}
	r.processOver.Store(false)

	return r
}

// JSON output structures

type ReportJSON struct {
	Summary    SummaryJSON           `json:"summary"`
	Histogram  []BucketJSON          `json:"histogram"`
	Percentiles map[string]int       `json:"percentiles"`
}

type SummaryJSON struct {
	Count         int `json:"count"`
	TotalBytes    int `json:"total_bytes"`
	SmallestBytes int `json:"smallest_bytes"`
	LargestBytes  int `json:"largest_bytes"`
	AverageBytes  int `json:"average_bytes"`
}

type BucketJSON struct {
	BucketStart int `json:"bucket_start"`
	BucketEnd   int `json:"bucket_end"`
	Count       int `json:"count"`
}

func (r *report) JSON() string {
	summary := SummaryJSON{
		Count:         r.stats.Count,
		TotalBytes:    r.stats.Total,
		SmallestBytes: r.stats.Smallest,
		LargestBytes:  r.stats.Largest,
		AverageBytes:  r.stats.Average,
	}

	// histogram buckets
	histogram := r.histogramJSON()

	// percentiles
	percentiles := r.percentilesJSON()

	report := ReportJSON{
		Summary:     summary,
		Histogram:   histogram,
		Percentiles: percentiles,
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Sprintf(`{"error": "json marshal failed: %v"}`, err)
	}
	return string(data)
}

func (r *report) histogramJSON() []BucketJSON {
	if r.bucketCount == 0 || r.stats.Largest < 0 {
		return nil
	}

	buckets := make([]int, r.bucketCount+1)
	counts := make([]int, r.bucketCount+1)
	bs := (r.stats.Largest - r.stats.Smallest) / r.bucketCount
	for i := 0; i < r.bucketCount; i++ {
		buckets[i] = r.stats.Smallest + bs*i
	}
	buckets[r.bucketCount] = r.stats.Largest

	var bi int
	for i := 0; i < len(r.stats.sizes); {
		s := r.stats.sizes[i]
		if s <= buckets[bi] {
			i++
			r.stats.countLock.RLock()
			counts[bi] += r.stats.sizeToCount[s]
			r.stats.countLock.RUnlock()
		} else if bi < len(buckets)-1 {
			bi++
		}
	}

	result := make([]BucketJSON, 0, r.bucketCount)
	for i := 0; i < r.bucketCount; i++ {
		result = append(result, BucketJSON{
			BucketStart: buckets[i],
			BucketEnd:   buckets[i+1],
			Count:       counts[i],
		})
	}
	return result
}

func (r *report) percentilesJSON() map[string]int {
	result := make(map[string]int)
	sort.Ints(r.stats.sizes)
	data := percentiles(r.stats.sizes, r.stats.sizeToCount)
	for i, p := range pctls {
		if i < len(data) {
			key := fmt.Sprintf("p%d", int(p))
			result[key] = data[i]
		}
	}
	return result
}
