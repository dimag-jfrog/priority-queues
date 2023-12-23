package priority_queues

import (
	"context"
	"reflect"
	"sort"
	"time"
)

type priorityBucket[T any] struct {
	QueueName string
	Value     int
	Capacity  int
	MsgsC     <-chan T
}

type level[T any] struct {
	TotalValue    int
	TotalCapacity int
	Buckets       []*priorityBucket[T]
}

type priorityQueuesByFreq[T any] struct {
	levels       []*level[T]
	totalBuckets int
}

type QueueFreqRatio[T any] struct {
	QueueName string
	MsgsC     <-chan T
	FreqRatio int
}

func newPriorityQueueByFrequencyRatio[T any](
	queuesWithFreqRatios []QueueFreqRatio[T]) *priorityQueuesByFreq[T] {
	zeroLevel := &level[T]{}
	zeroLevel.Buckets = make([]*priorityBucket[T], 0, len(queuesWithFreqRatios))
	for _, q := range queuesWithFreqRatios {
		zeroLevel.Buckets = append(zeroLevel.Buckets, &priorityBucket[T]{
			Value:     0,
			Capacity:  q.FreqRatio,
			MsgsC:     q.MsgsC,
			QueueName: q.QueueName,
		})
		zeroLevel.TotalCapacity += q.FreqRatio
	}
	sort.Slice(zeroLevel.Buckets, func(i int, j int) bool {
		return zeroLevel.Buckets[i].Capacity > zeroLevel.Buckets[j].Capacity
	})
	return &priorityQueuesByFreq[T]{
		levels:       []*level[T]{zeroLevel},
		totalBuckets: len(queuesWithFreqRatios),
	}
}

func ProcessMessagesByFrequencyRatio[T any](
	ctx context.Context,
	queuesWithFreqRatios []QueueFreqRatio[T],
	msgProcessor func(ctx context.Context, msg T, queueName string)) ExitReason {
	pq := newPriorityQueueByFrequencyRatio(queuesWithFreqRatios)
	return processPriorityQueueMessages[T](ctx, pq, msgProcessor)
}

func (pq *priorityQueuesByFreq[T]) ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent) {
	for numOfBucketsToProcess := 1; numOfBucketsToProcess <= pq.totalBuckets; numOfBucketsToProcess++ {
		selectCases := pq.prepareSelectCases(ctx, numOfBucketsToProcess)
		chosen, recv, recvOk := reflect.Select(selectCases)
		if chosen == 0 {
			// ctx.Done Channel
			return nil, &noMoreMessagesEvent{Reason: ContextCancelled}
		}
		isLastIteration := numOfBucketsToProcess == pq.totalBuckets
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
		levelIndex, bucketIndex := pq.getLevelAndBucketIndexByChosenChannelIndex(chosen)
		chosenBucket := pq.levels[levelIndex].Buckets[bucketIndex]
		res := &msgReceivedEvent[T]{Msg: msg, QueueName: chosenBucket.QueueName}
		pq.updateStateOnReceivingMessageToBucket(levelIndex, bucketIndex)
		return res, nil
	}
	return nil, nil
}

func (pq *priorityQueuesByFreq[T]) prepareSelectCases(ctx context.Context, numOfBucketsToProcess int) []reflect.SelectCase {
	addedBuckets := 0
	selectCases := make([]reflect.SelectCase, 0, numOfBucketsToProcess+2)
	selectCases = append(selectCases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(ctx.Done()),
	})
	for _, level := range pq.levels {
		for _, b := range level.Buckets {
			selectCases = append(selectCases, reflect.SelectCase{
				Dir:  reflect.SelectRecv,
				Chan: reflect.ValueOf(b.MsgsC),
			})
			addedBuckets++
			if addedBuckets == numOfBucketsToProcess {
				break
			}
		}
		if addedBuckets == numOfBucketsToProcess {
			break
		}
	}
	isLastIteration := numOfBucketsToProcess == pq.totalBuckets
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

func (pq *priorityQueuesByFreq[T]) getLevelAndBucketIndexByChosenChannelIndex(chosen int) (levelIndex int, bucketIndex int) {
	currIndex := 0
	for i := range pq.levels {
		for j := range pq.levels[i].Buckets {
			currIndex++
			if currIndex == chosen {
				return i, j
			}
		}
	}
	return -1, -1
}

func (pq *priorityQueuesByFreq[T]) updateStateOnReceivingMessageToBucket(levelIndex int, bucketIndex int) {
	chosenLevel := pq.levels[levelIndex]
	chosenBucket := chosenLevel.Buckets[bucketIndex]
	chosenBucket.Value++
	chosenLevel.TotalValue++

	if chosenLevel.TotalValue == chosenLevel.TotalCapacity {
		pq.mergeAllNextLevelsBackIntoCurrentLevel(levelIndex)
		return
	}
	if chosenBucket.Value == chosenBucket.Capacity {
		pq.moveBucketToNextLevel(levelIndex, bucketIndex)
		return
	}
}

func (pq *priorityQueuesByFreq[T]) mergeAllNextLevelsBackIntoCurrentLevel(levelIndex int) {
	chosenLevel := pq.levels[levelIndex]
	chosenLevel.TotalValue = 0
	for nextLevelIndex := levelIndex + 1; nextLevelIndex <= len(pq.levels)-1; nextLevelIndex++ {
		nextLevel := pq.levels[nextLevelIndex]
		chosenLevel.Buckets = append(chosenLevel.Buckets, nextLevel.Buckets...)
	}
	pq.levels = pq.levels[0 : levelIndex+1]
	for i := range chosenLevel.Buckets {
		chosenLevel.Buckets[i].Value = 0
	}
	sort.Slice(chosenLevel.Buckets, func(i int, j int) bool {
		return chosenLevel.Buckets[i].Capacity > chosenLevel.Buckets[j].Capacity
	})
}

func (pq *priorityQueuesByFreq[T]) moveBucketToNextLevel(levelIndex int, bucketIndex int) {
	chosenLevel := pq.levels[levelIndex]
	chosenBucket := chosenLevel.Buckets[bucketIndex]
	chosenBucket.Value = 0
	if levelIndex == len(pq.levels)-1 {
		pq.levels = append(pq.levels, &level[T]{})
	}
	nextLevel := pq.levels[levelIndex+1]
	nextLevel.TotalCapacity += chosenBucket.Capacity
	chosenLevel.Buckets = append(chosenLevel.Buckets[:bucketIndex], chosenLevel.Buckets[bucketIndex+1:]...)
	i := sort.Search(len(nextLevel.Buckets), func(i int) bool {
		return nextLevel.Buckets[i].Capacity < chosenBucket.Capacity
	})
	nextLevel.Buckets = append(nextLevel.Buckets, &priorityBucket[T]{})
	copy(nextLevel.Buckets[i+1:], nextLevel.Buckets[i:])
	nextLevel.Buckets[i] = chosenBucket
}
