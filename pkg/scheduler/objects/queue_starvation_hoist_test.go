/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package objects

import (
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-core/pkg/common/configs"
	"github.com/apache/yunikorn-core/pkg/common/resources"
)

// These tests cover the bounded-visit guarantee introduced by YUNIKORN-3243.
// They exercise the per-queue starvation clock, the parent-level sortQueues
// hoisting, and the configuration surface that controls both.

// TestStarvationDelayDefaultAndParsing verifies the default value and
// configuration parsing for the queue.starvation.delay property. The property
// must allow a zero value (feature disabled) unlike other delay-style
// properties that treat zero as invalid.
func TestStarvationDelayDefaultAndParsing(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	assert.Equal(t, root.GetStarvationDelay(), configs.DefaultQueueStarvationDelay,
		"default starvation delay should be used when no property is set")

	child, err := createManagedQueueWithProps(root, "delayed", true, nil, map[string]string{
		configs.QueueStarvationDelay: "5s",
	})
	assert.NilError(t, err, "child create failed")
	assert.Equal(t, child.GetStarvationDelay(), 5*time.Second,
		"starvation delay property should be parsed")

	disabled, err := createManagedQueueWithProps(root, "disabled", true, nil, map[string]string{
		configs.QueueStarvationDelay: "0s",
	})
	assert.NilError(t, err, "child create failed")
	assert.Equal(t, disabled.GetStarvationDelay(), time.Duration(0),
		"zero must be a valid value that disables hoisting")

	invalid, err := createManagedQueueWithProps(root, "invalid", true, nil, map[string]string{
		configs.QueueStarvationDelay: "not-a-duration",
	})
	assert.NilError(t, err, "child create failed")
	assert.Equal(t, invalid.GetStarvationDelay(), configs.DefaultQueueStarvationDelay,
		"invalid duration should fall back to default")

	negative, err := createManagedQueueWithProps(root, "negative", true, nil, map[string]string{
		configs.QueueStarvationDelay: "-1s",
	})
	assert.NilError(t, err, "child create failed")
	assert.Equal(t, negative.GetStarvationDelay(), configs.DefaultQueueStarvationDelay,
		"negative durations should fall back to default")
}

// TestStarvationClockTracksPendingTransitions ensures the internal clock is
// started when demand arrives on an empty queue, reset when demand drains, and
// not double-started while demand stays positive. These transitions are the
// basis of the bounded-visit guarantee.
func TestStarvationClockTracksPendingTransitions(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	parent, err := createManagedQueue(root, "parent", true, nil)
	assert.NilError(t, err, "parent create failed")
	leaf, err := createManagedQueue(parent, "leaf", false, nil)
	assert.NilError(t, err, "leaf create failed")

	assert.Assert(t, leaf.GetLastSchedulingAttempt().IsZero(),
		"clock must be zero before any demand arrives")

	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")

	before := time.Now()
	leaf.incPendingResource(res)
	after := time.Now()

	started := leaf.GetLastSchedulingAttempt()
	assert.Assert(t, !started.IsZero(), "clock must start when pending goes from zero to non-zero")
	assert.Assert(t, !started.Before(before) && !started.After(after),
		"clock must be set close to the moment demand arrived")

	// Adding more demand must not reset the clock; the grace window belongs to
	// the first transition.
	time.Sleep(2 * time.Millisecond)
	leaf.incPendingResource(res)
	assert.Equal(t, leaf.GetLastSchedulingAttempt(), started,
		"clock must not be reset while demand remains positive")

	leaf.decPendingResource(res)
	assert.Equal(t, leaf.GetLastSchedulingAttempt(), started,
		"clock must stay while pending is still positive")

	leaf.decPendingResource(res)
	assert.Assert(t, leaf.GetLastSchedulingAttempt().IsZero(),
		"clock must reset to zero when pending drops to zero")

	// Second transition starts a new clock.
	leaf.incPendingResource(res)
	second := leaf.GetLastSchedulingAttempt()
	assert.Assert(t, !second.IsZero(), "second transition must start the clock again")
}

// TestIsStarvedAt covers the predicate used by the parent's sort. A queue is
// starving only when all of: (a) starvation hoisting is enabled, (b) the queue
// has pending demand, (c) the clock has been started, and (d) the elapsed time
// exceeds the configured delay are true.
func TestIsStarvedAt(t *testing.T) {
	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	leaf, err := createManagedQueue(root, "leaf", false, nil)
	assert.NilError(t, err, "leaf create failed")

	now := time.Now()
	delay := 10 * time.Second

	assert.Assert(t, !leaf.isStarvedAt(now, 0),
		"delay=0 must disable the check regardless of other state")
	assert.Assert(t, !leaf.isStarvedAt(now, -time.Second),
		"negative delay must be treated as disabled")
	assert.Assert(t, !leaf.isStarvedAt(now, delay),
		"a queue with zero pending must never be considered starving")

	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "failed to create basic resource")
	leaf.incPendingResource(res)

	leaf.SetLastSchedulingAttempt(now.Add(-5 * time.Second))
	assert.Assert(t, !leaf.isStarvedAt(now, delay),
		"a queue visited within the delay must not be flagged as starving")

	leaf.SetLastSchedulingAttempt(now.Add(-11 * time.Second))
	assert.Assert(t, leaf.isStarvedAt(now, delay),
		"a queue skipped for longer than the delay must be flagged as starving")

	leaf.SetLastSchedulingAttempt(time.Time{})
	assert.Assert(t, !leaf.isStarvedAt(now, delay),
		"zero clock means 'not tracked' and must not trigger hoisting")
}

// TestHoistStarvedQueuesStablePartition locks in the ordering contract of
// hoistStarvedQueues: starved queues move to the front, but both the relative
// order of the starved and non-starved groups is preserved so that the DRF
// sort produced upstream still drives ties.
func TestHoistStarvedQueuesStablePartition(t *testing.T) {
	now := time.Now()
	delay := time.Second

	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")

	mkQueue := func(name string, starved bool) *Queue {
		q, qerr := createManagedQueue(root, name, false, nil)
		assert.NilError(t, qerr, "queue create failed")
		res, rerr := resources.NewResourceFromConf(map[string]string{"first": "1"})
		assert.NilError(t, rerr, "resource create failed")
		q.incPendingResource(res)
		if starved {
			q.SetLastSchedulingAttempt(now.Add(-2 * delay))
		} else {
			q.SetLastSchedulingAttempt(now)
		}
		return q
	}

	// Build: [fresh, starved, fresh, starved, fresh]. Expected after the
	// partition: [starved(1), starved(3), fresh(0), fresh(2), fresh(4)].
	a := mkQueue("a", false)
	b := mkQueue("b", true)
	c := mkQueue("c", false)
	d := mkQueue("d", true)
	e := mkQueue("e", false)

	ordered := []*Queue{a, b, c, d, e}
	hoistStarvedQueues(ordered, now, delay)

	names := make([]string, len(ordered))
	for i, q := range ordered {
		names[i] = q.Name
	}
	assert.DeepEqual(t, names, []string{"b", "d", "a", "c", "e"})
}

// TestHoistStarvedQueuesNoOpCases documents the early-return paths so that
// future refactors do not accidentally regress them.
func TestHoistStarvedQueuesNoOpCases(t *testing.T) {
	now := time.Now()

	hoistStarvedQueues(nil, now, time.Second)     // nil slice is safe
	hoistStarvedQueues([]*Queue{}, now, time.Second) // empty slice is safe

	root, err := createRootQueue(nil)
	assert.NilError(t, err, "queue create failed")
	single, err := createManagedQueue(root, "single", false, nil)
	assert.NilError(t, err, "queue create failed")
	single.SetLastSchedulingAttempt(now.Add(-time.Hour))
	res, err := resources.NewResourceFromConf(map[string]string{"first": "1"})
	assert.NilError(t, err, "resource create failed")
	single.incPendingResource(res)

	list := []*Queue{single}
	hoistStarvedQueues(list, now, time.Second)
	assert.Equal(t, list[0], single, "single-element slices are returned unchanged")

	// Disabled delay must be a no-op even if queues look starving.
	other, err := createManagedQueue(root, "other", false, nil)
	assert.NilError(t, err, "queue create failed")
	other.incPendingResource(res)
	other.SetLastSchedulingAttempt(now)
	pair := []*Queue{single, other}
	hoistStarvedQueues(pair, now, 0)
	assert.Equal(t, pair[0], single, "delay=0 must not reorder queues")
	assert.Equal(t, pair[1], other, "delay=0 must not reorder queues")
}

// TestSortQueuesHoistsStarvedSibling is the end-to-end proof for the bug in
// YUNIKORN-3243. The DRF sort would normally place the larger queue first
// because its allocated/guaranteed ratio is lower, but once the smaller queue
// has been skipped past the starvation delay the parent's sort must pull it
// back to the front so that the next TryAllocate cycle visits it.
func TestSortQueuesHoistsStarvedSibling(t *testing.T) {
	// Asymmetric guarantees reproduce the production 3600:1 ratio so the
	// small queue's fair-share ratio stays dominant for thousands of cycles.
	root, err := createRootQueueWithProps(map[string]string{
		configs.QueueStarvationDelay: "100ms",
	})
	assert.NilError(t, err, "root create failed")

	large, err := createManagedQueueGuaranteed(root, "large", false,
		map[string]string{"memory": "400G"},
		map[string]string{"memory": "360G"},
		nil)
	assert.NilError(t, err, "large create failed")
	small, err := createManagedQueueGuaranteed(root, "small", false,
		map[string]string{"memory": "100M"},
		map[string]string{"memory": "100M"},
		nil)
	assert.NilError(t, err, "small create failed")

	// Give small a prior allocation so small's ratio (0.1) dominates large's (0).
	priorAlloc, err := resources.NewResourceFromConf(map[string]string{"memory": "10M"})
	assert.NilError(t, err, "resource create failed")
	small.IncAllocatedResource(priorAlloc)

	largeDemand, err := resources.NewResourceFromConf(map[string]string{"memory": "1G"})
	assert.NilError(t, err, "resource create failed")
	smallDemand, err := resources.NewResourceFromConf(map[string]string{"memory": "1M"})
	assert.NilError(t, err, "resource create failed")
	large.incPendingResource(largeDemand)
	small.incPendingResource(smallDemand)

	// Baseline: both queues have freshly-started clocks, so the normal DRF
	// sort wins and large is visited first.
	fresh := time.Now()
	large.SetLastSchedulingAttempt(fresh)
	small.SetLastSchedulingAttempt(fresh)
	sorted := root.sortQueues()
	assert.Equal(t, len(sorted), 2, "both children should be eligible for scheduling")
	assert.Equal(t, sorted[0].Name, "large",
		"without hoisting the large queue must sort first under DRF")

	// Simulate small being skipped past the starvation threshold. On the next
	// sort it must be hoisted in front of large even though its DRF ratio is
	// still higher.
	small.SetLastSchedulingAttempt(time.Now().Add(-time.Second))
	sorted = root.sortQueues()
	assert.Equal(t, len(sorted), 2)
	assert.Equal(t, sorted[0].Name, "small",
		"starved child must be hoisted to the front regardless of DRF ratio")
	assert.Equal(t, sorted[1].Name, "large",
		"non-starved sibling must keep its original relative position")
}

// TestSortQueuesDisabledDelayKeepsDRF guards against accidentally enabling the
// hoist when an operator has turned it off. With delay=0 the DRF sort must be
// returned verbatim even for a queue that has been skipped for a long time.
func TestSortQueuesDisabledDelayKeepsDRF(t *testing.T) {
	root, err := createRootQueueWithProps(map[string]string{
		configs.QueueStarvationDelay: "0s",
	})
	assert.NilError(t, err, "root create failed")

	large, err := createManagedQueueGuaranteed(root, "large", false,
		map[string]string{"memory": "400G"},
		map[string]string{"memory": "360G"},
		nil)
	assert.NilError(t, err, "large create failed")
	small, err := createManagedQueueGuaranteed(root, "small", false,
		map[string]string{"memory": "100M"},
		map[string]string{"memory": "100M"},
		nil)
	assert.NilError(t, err, "small create failed")

	priorAlloc, err := resources.NewResourceFromConf(map[string]string{"memory": "10M"})
	assert.NilError(t, err, "resource create failed")
	small.IncAllocatedResource(priorAlloc)

	largeDemand, err := resources.NewResourceFromConf(map[string]string{"memory": "1G"})
	assert.NilError(t, err, "resource create failed")
	smallDemand, err := resources.NewResourceFromConf(map[string]string{"memory": "1M"})
	assert.NilError(t, err, "resource create failed")
	large.incPendingResource(largeDemand)
	small.incPendingResource(smallDemand)

	// Small has been "starving" for an hour, but the feature is disabled.
	small.SetLastSchedulingAttempt(time.Now().Add(-time.Hour))
	large.SetLastSchedulingAttempt(time.Now())

	sorted := root.sortQueues()
	assert.Equal(t, len(sorted), 2)
	assert.Equal(t, sorted[0].Name, "large",
		"disabled hoisting must leave the DRF order untouched")
}

// createRootQueueWithProps is a local helper until the shared utility in
// utilities_test.go grows a props-taking root helper. Kept in this file so it
// does not conflict with existing helpers.
func createRootQueueWithProps(props map[string]string) (*Queue, error) {
	return createManagedQueueWithProps(nil, "root", true, nil, props)
}
