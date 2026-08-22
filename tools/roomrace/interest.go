package main

import (
	"encoding/binary"
	"fmt"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
)

// interestWindow is how long after Subscribe() returns a publisher on the
// services connection actually starts reaching the new subscription. This is
// the floor under Problem 2: no client can subscribe faster than this.
type interestWindow struct {
	Samples       int   `json:"samples"`
	Delivered     int   `json:"delivered"`
	MedianMicros  int64 `json:"medianMicros"`
	P95Micros     int64 `json:"p95Micros"`
	MaxMicros     int64 `json:"maxMicros"`
	FlushRTTMicro int64 `json:"flushRttMicros"`
}

func measureInterestWindow(subURL string, pub *nats.Conn, samples int) (interestWindow, error) {
	sc, err := nats.Connect(subURL, nats.Name("roomrace-interest"))
	if err != nil {
		return interestWindow{}, fmt.Errorf("connect %s: %w", subURL, err)
	}
	defer sc.Close()

	out := interestWindow{Samples: samples}
	windows := make([]time.Duration, 0, samples)
	flushes := make([]time.Duration, 0, samples)

	for i := range samples {
		subj := fmt.Sprintf("roomrace.interest.%d.%d", time.Now().UnixNano(), i)
		stop := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(done)
			buf := make([]byte, 8)
			for {
				select {
				case <-stop:
					return
				default:
				}
				binary.BigEndian.PutUint64(buf, uint64(time.Now().UnixNano()))
				if err := pub.Publish(subj, buf); err != nil {
					return
				}
				if err := pub.Flush(); err != nil {
					return
				}
				time.Sleep(200 * time.Microsecond)
			}
		}()

		first := make(chan int64, 1)
		t0 := time.Now()
		sub, err := sc.Subscribe(subj, func(m *nats.Msg) {
			if len(m.Data) != 8 {
				return
			}
			select {
			case first <- int64(binary.BigEndian.Uint64(m.Data)):
			default:
			}
		})
		if err != nil {
			close(stop)
			<-done
			return interestWindow{}, fmt.Errorf("subscribe probe: %w", err)
		}

		select {
		case ns := <-first:
			d := time.Duration(ns - t0.UnixNano())
			if d < 0 {
				d = 0
			}
			windows = append(windows, d)
			out.Delivered++
		case <-time.After(3 * time.Second):
		}

		tf := time.Now()
		_ = sc.Flush()
		flushes = append(flushes, time.Since(tf))

		close(stop)
		<-done
		_ = sub.Unsubscribe()
	}

	out.MedianMicros = quantileMicros(windows, 0.5)
	out.P95Micros = quantileMicros(windows, 0.95)
	out.MaxMicros = quantileMicros(windows, 1.0)
	out.FlushRTTMicro = quantileMicros(flushes, 0.5)
	return out, nil
}

func quantileMicros(ds []time.Duration, q float64) int64 {
	if len(ds) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(q * float64(len(sorted)-1))
	return sorted[idx].Microseconds()
}
