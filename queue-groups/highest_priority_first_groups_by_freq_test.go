package queue_groups_test

import (
	"context"
	"fmt"
	"github.com/dimag-jfrog/priority-queues/queue-groups"
	"testing"
	"time"

	pq "github.com/dimag-jfrog/priority-queues"
)

func TestProcessMessagesByFreqRatioAmongHighestFirstQueueGroups(t *testing.T) {
	msgsChannels := make([]chan *Msg, 4)
	msgsChannels[0] = make(chan *Msg, 15)
	msgsChannels[1] = make(chan *Msg, 15)
	msgsChannels[2] = make(chan *Msg, 15)
	msgsChannels[3] = make(chan *Msg, 15)

	queues := []queue_groups.QueueGroupWithHighestPriorityFirst[*Msg]{
		{
			QueuesWithPriority: []pq.QueueWithPriority[*Msg]{
				{
					QueueName: "Priority-1",
					MsgsC:     msgsChannels[0],
					Priority:  1,
				},
				{
					QueueName: "Priority-5",
					MsgsC:     msgsChannels[1],
					Priority:  5,
				},
			},
			FreqRatio: 1,
		},
		{
			QueuesWithPriority: []pq.QueueWithPriority[*Msg]{
				{
					QueueName: "Priority-10",
					MsgsC:     msgsChannels[2],
					Priority:  10,
				},
			},
			FreqRatio: 5,
		},
		{
			QueuesWithPriority: []pq.QueueWithPriority[*Msg]{
				{
					QueueName: "Priority-1000",
					MsgsC:     msgsChannels[3],
					Priority:  1000,
				},
			},
			FreqRatio: 10,
		},
	}

	queueNames := []string{"Priority-1", "Priority-5", "Priority-10", "Priority-1000"}

	for i := 0; i <= 2; i++ {
		for j := 1; j <= 15; j++ {
			msgsChannels[i] <- &Msg{Body: fmt.Sprintf("%s Msg-%d", queueNames[i], j)}
		}
	}
	msgsChannels[3] <- &Msg{Body: "Priority-1000 Msg-1"}

	var results []*Msg
	msgProcessor := func(_ context.Context, msg *Msg, queueName string) {
		results = append(results, msg)
	}
	ctx, cancel := context.WithCancel(context.Background())

	go queue_groups.ProcessMessagesByFreqRatioAmongHighestFirstQueueGroups(ctx, queues, msgProcessor)

	time.Sleep(3 * time.Second)
	cancel()

	expectedResults := []*Msg{
		{Body: "Priority-1000 Msg-1"},
		{Body: "Priority-10 Msg-1"},
		{Body: "Priority-10 Msg-2"},
		{Body: "Priority-10 Msg-3"},
		{Body: "Priority-10 Msg-4"},
		{Body: "Priority-10 Msg-5"},
		{Body: "Priority-5 Msg-1"},
		{Body: "Priority-10 Msg-6"},
		{Body: "Priority-10 Msg-7"},
		{Body: "Priority-10 Msg-8"},
		{Body: "Priority-10 Msg-9"},
		{Body: "Priority-10 Msg-10"},
		{Body: "Priority-5 Msg-2"},
		{Body: "Priority-10 Msg-11"},
		{Body: "Priority-10 Msg-12"},
		{Body: "Priority-10 Msg-13"},
		{Body: "Priority-10 Msg-14"},
		{Body: "Priority-10 Msg-15"},
		{Body: "Priority-5 Msg-3"},
		{Body: "Priority-5 Msg-4"},
		{Body: "Priority-5 Msg-5"},
		{Body: "Priority-5 Msg-6"},
		{Body: "Priority-5 Msg-7"},
		{Body: "Priority-5 Msg-8"},
		{Body: "Priority-5 Msg-9"},
		{Body: "Priority-5 Msg-10"},
		{Body: "Priority-5 Msg-11"},
		{Body: "Priority-5 Msg-12"},
		{Body: "Priority-5 Msg-13"},
		{Body: "Priority-5 Msg-14"},
		{Body: "Priority-5 Msg-15"},
		{Body: "Priority-1 Msg-1"},
		{Body: "Priority-1 Msg-2"},
		{Body: "Priority-1 Msg-3"},
		{Body: "Priority-1 Msg-4"},
		{Body: "Priority-1 Msg-5"},
		{Body: "Priority-1 Msg-6"},
		{Body: "Priority-1 Msg-7"},
		{Body: "Priority-1 Msg-8"},
		{Body: "Priority-1 Msg-9"},
		{Body: "Priority-1 Msg-10"},
		{Body: "Priority-1 Msg-11"},
		{Body: "Priority-1 Msg-12"},
		{Body: "Priority-1 Msg-13"},
		{Body: "Priority-1 Msg-14"},
		{Body: "Priority-1 Msg-15"},
	}

	if len(results) != len(expectedResults) {
		t.Errorf("Expected %d results, but got %d", len(expectedResults), len(results))
	}
	for i := range results {
		if results[i].Body != expectedResults[i].Body {
			t.Errorf("Result %d: Expected message %s, but got %s",
				i, expectedResults[i].Body, results[i].Body)
		}
	}
}
