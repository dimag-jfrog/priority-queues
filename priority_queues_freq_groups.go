package priority_queues

import (
	"context"
	"reflect"
	"sort"
	"time"
)

type PriorityQueueGroupWithFreqRatio[T any] struct {
	QueuesWithFreqRatios []QueueFreqRatio[T]
	Priority             int
}

type msgWithQueueName[T any] struct {
	Msg       T
	QueueName string
}

type priorityQueueGroupAggregatedChannel[T any] struct {
	Priority    int
	AggregatedC <-chan msgWithQueueName[T]
}

type priorityQueuesGroupHighestFirst[T any] struct {
	queuesGroups []*priorityQueueGroupAggregatedChannel[T]
}

func newPriorityQueuesGroupByPriority[T any](
	ctx context.Context,
	queuesGroupsWithPriorities []PriorityQueueGroupWithFreqRatio[T]) *priorityQueuesGroupHighestFirst[T] {
	pq := &priorityQueuesGroupHighestFirst[T]{
		queuesGroups: make([]*priorityQueueGroupAggregatedChannel[T], 0, len(queuesGroupsWithPriorities)),
	}

	for _, q := range queuesGroupsWithPriorities {
		aggregatedC := make(chan msgWithQueueName[T])
		if len(q.QueuesWithFreqRatios) == 1 {
			queue := q.QueuesWithFreqRatios[0]
			go func() {
				for {
					select {
					case <-ctx.Done():
						return
					case msg, ok := <-queue.MsgsC:
						if !ok {
							close(aggregatedC)
							return
						}
						aggregatedC <- msgWithQueueName[T]{Msg: msg, QueueName: queue.QueueName}
					}
				}
			}()
		} else {
			msgProcessor := func(_ context.Context, msg T, queueName string) {
				aggregatedC <- msgWithQueueName[T]{Msg: msg, QueueName: queueName}
			}
			go ProcessMessagesByFrequencyRatio(ctx, q.QueuesWithFreqRatios, msgProcessor)
		}

		pq.queuesGroups = append(pq.queuesGroups, &priorityQueueGroupAggregatedChannel[T]{
			AggregatedC: aggregatedC,
			Priority:    q.Priority,
		})
	}
	sort.Slice(pq.queuesGroups, func(i int, j int) bool {
		return pq.queuesGroups[i].Priority > pq.queuesGroups[j].Priority
	})
	return pq
}

func ProcessMessagesByPriorityAmongFreqRatioQueueGroups[T any](
	ctx context.Context,
	queuesGroupsWithFreqRatios []PriorityQueueGroupWithFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, queueName string)) ExitReason {
	pq := newPriorityQueuesGroupByPriority(ctx, queuesGroupsWithFreqRatios)
	return processPriorityQueueMessages[T](ctx, pq, msgProcessor)
}

func (pq *priorityQueuesGroupHighestFirst[T]) ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent) {
	for nextPriorityQueuesGroupIndex := 0; nextPriorityQueuesGroupIndex < len(pq.queuesGroups); nextPriorityQueuesGroupIndex++ {
		selectCases := pq.prepareSelectCases(ctx, nextPriorityQueuesGroupIndex)
		chosen, recv, recvOk := reflect.Select(selectCases)
		if chosen == 0 {
			// ctx.Done Channel
			return nil, &noMoreMessagesEvent{Reason: ContextCancelled}
		}
		isLastIteration := nextPriorityQueuesGroupIndex == len(pq.queuesGroups)-1
		if !isLastIteration && chosen == len(selectCases)-1 {
			// Default case - go to next iteration to increase the range of allowed minimal priority queues
			// on last iteration - blocking wait on all receive channels without default case
			continue
		}
		if !recvOk {
			// no more messages in channel
			return nil, &noMoreMessagesEvent{Reason: ChannelClosed}
		}
		// Message received successfully
		msg := recv.Interface().(msgWithQueueName[T])
		res := &msgReceivedEvent[T]{Msg: msg.Msg, QueueName: msg.QueueName}
		return res, nil
	}
	return nil, nil
}

func (pq *priorityQueuesGroupHighestFirst[T]) prepareSelectCases(ctx context.Context, currPriorityQueuesGroupIndex int) []reflect.SelectCase {
	var selectCases []reflect.SelectCase
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for i := 0; i <= currPriorityQueuesGroupIndex; i++ {
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(pq.queuesGroups[i].AggregatedC),
		})
		continue
	}
	isLastIteration := currPriorityQueuesGroupIndex == len(pq.queuesGroups)-1
	if !isLastIteration {
		selectCases = append(selectCases, reflect.SelectCase{
			// The default option without any sleep did not pass tests
			// short sleep is needed to guarantee that we do not enter default case when there are still messages
			// in the deliveries channel that can be retrieved
			//Dir: reflect.SelectDefault,
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(time.After(100 * time.Microsecond)),
		})
	}
	return selectCases
}
