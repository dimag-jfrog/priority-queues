package queue_groups

import (
	"context"
	"github.com/dimag-jfrog/priority-queues"
	"sort"
)

type PriorityQueueGroupWithFreqRatio[T any] struct {
	QueuesWithFreqRatios []priority_queues.QueueFreqRatio[T]
	Priority             int
}

func newPriorityQueuesGroupByPriority[T any](
	ctx context.Context,
	queuesGroupsWithPriorities []PriorityQueueGroupWithFreqRatio[T]) []priority_queues.QueueWithPriority[msgWithQueueName[T]] {
	res := make([]priority_queues.QueueWithPriority[msgWithQueueName[T]], 0, len(queuesGroupsWithPriorities))

	for _, q := range queuesGroupsWithPriorities {
		aggregatedC := make(chan msgWithQueueName[T])
		if len(q.QueuesWithFreqRatios) == 1 {
			queue := q.QueuesWithFreqRatios[0]
			go messagesChannelToMessagesWithQueueNameChannel(ctx, queue.QueueName, queue.MsgsC, aggregatedC)
		} else {
			msgProcessor := func(_ context.Context, msg T, queueName string) {
				aggregatedC <- msgWithQueueName[T]{Msg: msg, QueueName: queueName}
			}
			go priority_queues.ProcessMessagesByFrequencyRatio(ctx, q.QueuesWithFreqRatios, msgProcessor)
		}

		res = append(res, priority_queues.QueueWithPriority[msgWithQueueName[T]]{
			MsgsC:    aggregatedC,
			Priority: q.Priority,
		})
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].Priority > res[j].Priority
	})
	return res
}

func ProcessMessagesByPriorityAmongFreqRatioQueueGroups[T any](
	ctx context.Context,
	queuesGroupsWithFreqRatios []PriorityQueueGroupWithFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, queueName string)) priority_queues.ExitReason {
	queues := newPriorityQueuesGroupByPriority(ctx, queuesGroupsWithFreqRatios)
	msgProcessorNew := func(_ context.Context, msg msgWithQueueName[T], queueName string) {
		msgProcessor(ctx, msg.Msg, msg.QueueName)
	}
	return priority_queues.ProcessMessagesByPriorityWithHighestAlwaysFirst[msgWithQueueName[T]](ctx, queues, msgProcessorNew)
}
