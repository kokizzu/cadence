// The MIT License (MIT)

// Copyright (c) 2017-2020 Uber Technologies Inc.

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package queuev2

import (
	"context"
	"time"

	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/dynamicconfig/dynamicproperties"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/service/history/queue"
	"github.com/uber/cadence/service/history/shard"
	"github.com/uber/cadence/service/history/task"
)

type (
	Options struct {
		PageSize                             dynamicproperties.IntPropertyFn
		DeleteBatchSize                      dynamicproperties.IntPropertyFn
		MaxPollRPS                           dynamicproperties.IntPropertyFn
		MaxPollInterval                      dynamicproperties.DurationPropertyFn
		MaxPollIntervalJitterCoefficient     dynamicproperties.FloatPropertyFn
		UpdateAckInterval                    dynamicproperties.DurationPropertyFn
		UpdateAckIntervalJitterCoefficient   dynamicproperties.FloatPropertyFn
		RedispatchInterval                   dynamicproperties.DurationPropertyFn
		MaxPendingTasksCount                 dynamicproperties.IntPropertyFn
		PollBackoffInterval                  dynamicproperties.DurationPropertyFn
		PollBackoffIntervalJitterCoefficient dynamicproperties.FloatPropertyFn

		EnableValidator        dynamicproperties.BoolPropertyFn
		ValidationInterval     dynamicproperties.DurationPropertyFn
		MaxStartJitterInterval dynamicproperties.DurationPropertyFn
	}

	queueBase struct {
		shard           shard.Context
		taskProcessor   task.Processor
		logger          log.Logger
		metricsClient   metrics.Client
		metricsScope    metrics.Scope
		category        persistence.HistoryTaskCategory
		options         *Options
		timeSource      clock.TimeSource
		taskInitializer task.Initializer

		redispatcher          task.Redispatcher
		queueReader           QueueReader
		monitor               Monitor
		updateQueueStateTimer clock.Timer
		virtualQueueManager   VirtualQueueManager
		exclusiveAckLevel     persistence.HistoryTaskKey

		newVirtualSliceState VirtualSliceState
	}
)

func newQueueBase(
	shard shard.Context,
	taskProcessor task.Processor,
	logger log.Logger,
	metricsClient metrics.Client,
	metricsScope metrics.Scope,
	category persistence.HistoryTaskCategory,
	taskExecutor task.Executor,
	options *Options,
) *queueBase {
	timeSource := shard.GetTimeSource()
	persistenceQueueState, err := shard.GetQueueState(category)
	if err != nil {
		logger.Fatal("Failed to get queue state, probably task category is not supported", tag.Error(err), tag.Dynamic("category", category))
	}
	queueState := FromPersistenceQueueState(persistenceQueueState)
	exclusiveAckLevel := getExclusiveAckLevelFromQueueState(queueState)

	redispatcher := task.NewRedispatcher(
		taskProcessor,
		timeSource,
		&task.RedispatcherOptions{
			TaskRedispatchInterval: options.RedispatchInterval,
		},
		logger,
		metricsScope,
	)
	var queueType task.QueueType
	if category == persistence.HistoryTaskCategoryTransfer {
		queueType = task.QueueTypeActiveTransfer
	} else if category == persistence.HistoryTaskCategoryTimer {
		queueType = task.QueueTypeActiveTimer
	}
	taskInitializer := func(t persistence.Task) task.Task {
		return task.NewHistoryTask(
			shard,
			t,
			queueType,
			task.InitializeLoggerForTask(shard.GetShardID(), t, logger),
			func(task persistence.Task) (bool, error) { return true, nil },
			taskExecutor,
			taskProcessor,
			redispatcher,
			shard.GetConfig().TaskCriticalRetryCount,
		)
	}
	queueReader := NewQueueReader(
		shard,
		category,
	)
	monitor := NewMonitor(category)
	virtualQueueManager := NewVirtualQueueManager(
		taskProcessor,
		redispatcher,
		taskInitializer,
		queueReader,
		logger,
		metricsScope,
		timeSource,
		quotas.NewDynamicRateLimiter(options.MaxPollRPS.AsFloat64()),
		monitor,
		&VirtualQueueOptions{
			PageSize:                             options.PageSize,
			MaxPendingTasksCount:                 options.MaxPendingTasksCount,
			PollBackoffInterval:                  options.PollBackoffInterval,
			PollBackoffIntervalJitterCoefficient: options.PollBackoffIntervalJitterCoefficient,
		},
		queueState.VirtualQueueStates,
	)
	return &queueBase{
		shard:               shard,
		taskProcessor:       taskProcessor,
		logger:              logger,
		metricsClient:       metricsClient,
		metricsScope:        metricsScope,
		category:            category,
		options:             options,
		timeSource:          timeSource,
		taskInitializer:     taskInitializer,
		redispatcher:        redispatcher,
		queueReader:         queueReader,
		monitor:             monitor,
		exclusiveAckLevel:   exclusiveAckLevel,
		virtualQueueManager: virtualQueueManager,
		newVirtualSliceState: VirtualSliceState{
			Range: Range{
				InclusiveMinTaskKey: queueState.ExclusiveMaxReadLevel,
				ExclusiveMaxTaskKey: persistence.MaximumHistoryTaskKey,
			},
			Predicate: NewUniversalPredicate(),
		},
	}
}

func (q *queueBase) Start() {
	q.redispatcher.Start()
	q.virtualQueueManager.Start()

	q.updateQueueStateTimer = q.timeSource.NewTimer(backoff.JitDuration(
		q.options.UpdateAckInterval(),
		q.options.UpdateAckIntervalJitterCoefficient(),
	))
}

func (q *queueBase) Stop() {
	q.updateQueueStateTimer.Stop()
	q.virtualQueueManager.Stop()
	q.redispatcher.Stop()
}

func (q *queueBase) Category() persistence.HistoryTaskCategory {
	return q.category
}

func (q *queueBase) FailoverDomain(domainIDs map[string]struct{}) {}

func (q *queueBase) HandleAction(ctx context.Context, clusterName string, action *queue.Action) (*queue.ActionResult, error) {
	return nil, nil
}

func (q *queueBase) LockTaskProcessing() {}

func (q *queueBase) UnlockTaskProcessing() {}

func (q *queueBase) processNewTasks() bool {
	newExclusiveMaxTaskKey := q.shard.UpdateIfNeededAndGetQueueMaxReadLevel(q.category, q.shard.GetClusterMetadata().GetCurrentClusterName())
	if q.category.Type() == persistence.HistoryTaskCategoryTypeImmediate {
		newExclusiveMaxTaskKey = persistence.NewImmediateTaskKey(newExclusiveMaxTaskKey.GetTaskID() + 1)
	}

	newVirtualSliceState, remainingVirtualSliceState, ok := q.newVirtualSliceState.TrySplitByTaskKey(newExclusiveMaxTaskKey)
	if !ok {
		return false
	}
	q.newVirtualSliceState = remainingVirtualSliceState

	newVirtualSlice := NewVirtualSlice(newVirtualSliceState, q.taskInitializer, q.queueReader, NewPendingTaskTracker())

	q.logger.Debug("processing new tasks", tag.Dynamic("inclusiveMinTaskKey", newVirtualSliceState.Range.InclusiveMinTaskKey), tag.Dynamic("exclusiveMaxTaskKey", newVirtualSliceState.Range.ExclusiveMaxTaskKey))
	q.virtualQueueManager.AddNewVirtualSliceToRootQueue(newVirtualSlice)
	return true
}

func (q *queueBase) updateQueueState(ctx context.Context) {
	q.metricsScope.IncCounter(metrics.AckLevelUpdateCounter)
	queueState := &QueueState{
		VirtualQueueStates:    q.virtualQueueManager.UpdateAndGetState(),
		ExclusiveMaxReadLevel: q.newVirtualSliceState.Range.InclusiveMinTaskKey,
	}
	newExclusiveAckLevel := getExclusiveAckLevelFromQueueState(queueState)

	// for backward compatibility, we record the timer metrics in shard info scope
	pendingTaskCount := q.monitor.GetTotalPendingTaskCount()
	if q.category == persistence.HistoryTaskCategoryTransfer {
		q.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTransferActivePendingTasksTimer, time.Duration(pendingTaskCount))
	} else if q.category == persistence.HistoryTaskCategoryTimer {
		q.metricsClient.RecordTimer(metrics.ShardInfoScope, metrics.ShardInfoTimerActivePendingTasksTimer, time.Duration(pendingTaskCount))
	}

	// we emit the metrics in the queue scope and experiment with gauge metrics
	// TODO: review the metrics and remove this comment or change the metric from gauge to histogram
	q.metricsScope.UpdateGauge(metrics.PendingTaskGauge, float64(pendingTaskCount))

	if newExclusiveAckLevel.Compare(q.exclusiveAckLevel) > 0 {
		inclusiveMinTaskKey := q.exclusiveAckLevel
		exclusiveMaxTaskKey := newExclusiveAckLevel
		if q.category.Type() == persistence.HistoryTaskCategoryTypeScheduled {
			inclusiveMinTaskKey = persistence.NewHistoryTaskKey(inclusiveMinTaskKey.GetScheduledTime(), 0)
			exclusiveMaxTaskKey = persistence.NewHistoryTaskKey(exclusiveMaxTaskKey.GetScheduledTime(), 0)
		}
		for {
			pageSize := q.options.DeleteBatchSize()
			resp, err := q.shard.GetExecutionManager().RangeCompleteHistoryTask(ctx, &persistence.RangeCompleteHistoryTaskRequest{
				TaskCategory:        q.category,
				InclusiveMinTaskKey: inclusiveMinTaskKey,
				ExclusiveMaxTaskKey: exclusiveMaxTaskKey,
				PageSize:            pageSize,
			})
			if err != nil {
				q.logger.Error("Failed to range complete history tasks", tag.Error(err))
				return
			}
			if !persistence.HasMoreRowsToDelete(resp.TasksCompleted, pageSize) {
				break
			}
		}
		q.exclusiveAckLevel = newExclusiveAckLevel
	}

	// even though the ack level is not updated, we still need to update the queue state
	err := q.shard.UpdateQueueState(q.category, ToPersistenceQueueState(queueState))
	if err != nil {
		q.logger.Error("Failed to update queue state", tag.Error(err))
		q.metricsScope.IncCounter(metrics.AckLevelUpdateFailedCounter)
	}

	q.updateQueueStateTimer.Reset(backoff.JitDuration(
		q.options.UpdateAckInterval(),
		q.options.UpdateAckIntervalJitterCoefficient(),
	))
}

func getExclusiveAckLevelFromQueueState(state *QueueState) persistence.HistoryTaskKey {
	newExclusiveAckLevel := state.ExclusiveMaxReadLevel
	for _, virtualQueueState := range state.VirtualQueueStates {
		if len(virtualQueueState) != 0 {
			newExclusiveAckLevel = persistence.MinHistoryTaskKey(newExclusiveAckLevel, virtualQueueState[0].Range.InclusiveMinTaskKey)
		}
	}
	return newExclusiveAckLevel
}
