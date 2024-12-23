package queue_groups

import "context"

type msgWithQueueName[T any] struct {
	Msg       T
	QueueName string
}

func messagesChannelToMessagesWithQueueNameChannel[T any](
	ctx context.Context,
	queueName string,
	msgsC <-chan T,
	msgsWithQueueNameC chan<- msgWithQueueName[T]) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgsC:
			if !ok {
				close(msgsWithQueueNameC)
				return
			}
			msgsWithQueueNameC <- msgWithQueueName[T]{Msg: msg, QueueName: queueName}
		}
	}
}
