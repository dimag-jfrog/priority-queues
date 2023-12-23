package priority_queues

import (
	"context"
)

type priorityQueueMsgReceiver[T any] interface {
	ReceiveSingleMessage(ctx context.Context) (msgReceived *msgReceivedEvent[T], noMoreMessages *noMoreMessagesEvent)
}

type msgReceivedEvent[T any] struct {
	Msg       T
	QueueName string
}

type ExitReason int

const (
	ContextCancelled ExitReason = iota
	ChannelClosed
)

type noMoreMessagesEvent struct {
	Reason ExitReason
}

func processPriorityQueueMessages[T any](
	ctx context.Context,
	msgReceiver priorityQueueMsgReceiver[T],
	msgProcessor func(ctx context.Context, msg T, queueName string)) ExitReason {
	for {
		msgReceived, noMoreMessages := msgReceiver.ReceiveSingleMessage(ctx)
		if noMoreMessages != nil {
			return noMoreMessages.Reason
		}
		if msgReceived == nil {
			continue
		}
		msgProcessor(ctx, msgReceived.Msg, msgReceived.QueueName)
	}
}
