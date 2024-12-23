package queue_groups

import (
	"context"
	"github.com/dimag-jfrog/priority-queues"
	"sort"
)

type QueueGroupWithHighestPriorityFirst[T any] struct {
	QueuesWithPriority []priority_queues.QueueWithPriority[T]
	FreqRatio          int
}

func newPriorityQueuesGroupByFreqRatio[T any](
	ctx context.Context,
	queuesGroupsWithFreqRatio []QueueGroupWithHighestPriorityFirst[T]) []priority_queues.QueueFreqRatio[msgWithQueueName[T]] {
	res := make([]priority_queues.QueueFreqRatio[msgWithQueueName[T]], 0, len(queuesGroupsWithFreqRatio))

	for _, q := range queuesGroupsWithFreqRatio {
		aggregatedC := make(chan msgWithQueueName[T])
		if len(q.QueuesWithPriority) == 1 {
			queue := q.QueuesWithPriority[0]
			go messagesChannelToMessagesWithQueueNameChannel(ctx, queue.QueueName, queue.MsgsC, aggregatedC)
		} else {
			msgProcessor := func(_ context.Context, msg T, queueName string) {
				aggregatedC <- msgWithQueueName[T]{Msg: msg, QueueName: queueName}
			}
			go priority_queues.ProcessMessagesByPriorityWithHighestAlwaysFirst(ctx, q.QueuesWithPriority, msgProcessor)
		}

		res = append(res, priority_queues.QueueFreqRatio[msgWithQueueName[T]]{
			MsgsC:     aggregatedC,
			FreqRatio: q.FreqRatio,
		})
	}
	sort.Slice(res, func(i int, j int) bool {
		return res[i].FreqRatio > res[j].FreqRatio
	})
	return res
}

func ProcessMessagesByFreqRatioAmongHighestFirstQueueGroups[T any](
	ctx context.Context,
	queuesGroupsWithHighestPriorityFirst []QueueGroupWithHighestPriorityFirst[T],
	msgProcessor func(ctx context.Context, msg T, queueName string)) priority_queues.ExitReason {
	queues := newPriorityQueuesGroupByFreqRatio(ctx, queuesGroupsWithHighestPriorityFirst)
	msgProcessorNew := func(_ context.Context, msg msgWithQueueName[T], queueName string) {
		msgProcessor(ctx, msg.Msg, msg.QueueName)
	}
	return priority_queues.ProcessMessagesByFrequencyRatio[msgWithQueueName[T]](ctx, queues, msgProcessorNew)
}
