package priority_queues

import (
	"context"
	"reflect"
	"sort"
	"time"
)

type QueueWithPriority[T any] struct {
	QueueName string
	MsgsC     <-chan T
	Priority  int
}

type priorityQueuesHighestFirst[T any] struct {
	queues []*QueueWithPriority[T]
}

func newPriorityQueueByPriority[T any](
	queuesWithPriorities []QueueWithPriority[T]) *priorityQueuesHighestFirst[T] {
	pq := &priorityQueuesHighestFirst[T]{
		queues: make([]*QueueWithPriority[T], 0, len(queuesWithPriorities)),
	}

	for _, q := range queuesWithPriorities {
		pq.queues = append(pq.queues, &QueueWithPriority[T]{
			QueueName: q.QueueName,
			MsgsC:     q.MsgsC,
			Priority:  q.Priority,
		})
	}
	sort.Slice(pq.queues, func(i int, j int) bool {
		return pq.queues[i].Priority > pq.queues[j].Priority
	})
	return pq
}

func ProcessMessagesByPriorityWithHighestAlwaysFirst[T any](
	ctx context.Context,
	queuesWithPriorities []QueueWithPriority[T],
	msgProcessor func(ctx context.Context, msg T, queueName string)) ExitReason {
	pq := newPriorityQueueByPriority(queuesWithPriorities)
	return processPriorityQueueMessages[T](ctx, pq, msgProcessor)
}

func (pq *priorityQueuesHighestFirst[T]) ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent) {
	for nextPriorityQueueIndex := 0; nextPriorityQueueIndex < len(pq.queues); nextPriorityQueueIndex++ {
		selectCases := pq.prepareSelectCases(ctx, nextPriorityQueueIndex)
		chosen, recv, recvOk := reflect.Select(selectCases)
		if chosen == 0 {
			// ctx.Done Channel
			return nil, &noMoreMessagesEvent{Reason: ContextCancelled}
		}
		isLastIteration := nextPriorityQueueIndex == len(pq.queues)-1
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
		msg := recv.Interface().(T)
		res := &msgReceivedEvent[T]{Msg: msg, QueueName: pq.queues[chosen-1].QueueName}
		return res, nil
	}
	return nil, nil
}

func (pq *priorityQueuesHighestFirst[T]) prepareSelectCases(ctx context.Context, currPriorityQueueIndex int) []reflect.SelectCase {
	var selectCases []reflect.SelectCase
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for i := 0; i <= currPriorityQueueIndex; i++ {
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(pq.queues[i].MsgsC),
		})
	}
	isLastIteration := currPriorityQueueIndex == len(pq.queues)-1
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
