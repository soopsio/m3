// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"
)

func TestNewConfiguredChainedEnforcer(t *testing.T) {

}

func setupGlobalReporter() (tally.TestScope, *globalReporter) {
	s := tally.NewTestScope("", nil)
	gr := newGlobalReporter(s)
	return s, gr
}

func TestGlobalReporter_ReportCurrent(t *testing.T) {
	s, gr := setupGlobalReporter()

	gr.ReportCurrent(5.0)
	assertHasGauge(t, s.Snapshot(), tally.KeyForPrefixedStringMap(datapointsMetric, nil), 5.0)
}

func TestGlobalReporter_ReportCost(t *testing.T) {
	t.Run("reports positive", func(t *testing.T) {
		s, gr := setupGlobalReporter()
		gr.ReportCost(5.0)
		assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(datapointsCounterMetric, nil), 5.0)
	})

	t.Run("skips negative", func(t *testing.T) {
		s, gr := setupGlobalReporter()
		gr.ReportCost(-5.0)

		assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(datapointsCounterMetric, nil), 0.0)
	})
}

func TestGlobalReporter_ReportOverLimit(t *testing.T) {
	s, gr := setupGlobalReporter()
	gr.ReportOverLimit(true)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)
}

func setupPerQueryReporter() (tally.TestScope, *perQueryReporter) {
	s := tally.NewTestScope("", nil)
	gr := newPerQueryReporter(s)
	return s, gr
}

func TestPerQueryReporter_ReportOverLimit(t *testing.T) {
	s, pqr := setupPerQueryReporter()
	pqr.ReportOverLimit(true)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)
}

func TestPerQueryReporter_OnRelease(t *testing.T) {
	s, pqr := setupPerQueryReporter()
	pqr.OnChildRelease(5.0)
	pqr.OnChildRelease(110.0)

	// ignores current cost
	pqr.OnRelease(100.0)
	assertHasHistogram(t, s.Snapshot(),
		tally.KeyForPrefixedStringMap(maxDatapointsHistMetric, nil),
		map[float64]int64{
			1000.0: 1,
		})
}

func TestPerQueryReporter_OnChildRelease(t *testing.T) {
	_, pqr := setupPerQueryReporter()
	pqr.OnChildRelease(5.0)
	pqr.OnChildRelease(110.0)

	assert.InDelta(t, 110.0, float64(pqr.maxDatapoints), 0.0001)
}

func TestOverLimitReporter_ReportOverLimit(t *testing.T) {
	s := tally.NewTestScope("", nil)
	orl := newOverLimitReporter(s)

	orl.ReportOverLimit(true)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)

	orl.ReportOverLimit(false)
	assertHasCounter(t, s.Snapshot(), tally.KeyForPrefixedStringMap(queriesOverLimitMetric, map[string]string{
		"enabled": "true",
	}), 1)
}

func assertHasCounter(t *testing.T, snapshot tally.Snapshot, key string, v int) {
	counters := snapshot.Counters()
	if !assert.Contains(t, counters, key, "No such metric: %s", key) {
		return
	}

	counter := counters[key]

	assert.Equal(t, int(counter.Value()), v, "Incorrect value for counter %s", key)
}

func assertHasGauge(t *testing.T, snapshot tally.Snapshot, key string, v int) {
	gauges := snapshot.Gauges()
	if !assert.Contains(t, gauges, key, "No such metric: %s", key) {
		return
	}

	gauge := gauges[key]

	assert.Equal(t, int(gauge.Value()), v, "Incorrect value for gauge %s", key)
}

func assertHasHistogram(t *testing.T, snapshot tally.Snapshot, key string, values map[float64]int64) {
	histograms := snapshot.Histograms()
	if !assert.Contains(t, histograms, key, "No such metric: %s", key) {
		return
	}

	hist := histograms[key]

	actualValues := hist.Values()

	// filter zero values
	for k, v := range actualValues {
		if v == 0 {
			delete(actualValues, k)
		}
	}

	assert.Equal(t, values, actualValues)
}
