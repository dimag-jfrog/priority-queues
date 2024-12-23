package priority_queues_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pq "github.com/dimag-jfrog/priority-queues"
)

func TestProcessMessagesByPriorityAmongFreqRatioQueueGroups(t *testing.T) {
	msgsChannels := make([]chan *Msg, 4)
	msgsChannels[0] = make(chan *Msg, 15)
	msgsChannels[1] = make(chan *Msg, 15)
	msgsChannels[2] = make(chan *Msg, 15)
	msgsChannels[3] = make(chan *Msg, 15)

	queues := []pq.PriorityQueueGroupWithFreqRatio[*Msg]{
		{
			QueuesWithFreqRatios: []pq.QueueFreqRatio[*Msg]{
				{
					QueueName: "Priority-1",
					MsgsC:     msgsChannels[0],
					FreqRatio: 1,
				},
				{
					QueueName: "Priority-5",
					MsgsC:     msgsChannels[1],
					FreqRatio: 5,
				},
			},
			Priority: 1,
		},
		{
			QueuesWithFreqRatios: []pq.QueueFreqRatio[*Msg]{
				{
					QueueName: "Priority-10",
					MsgsC:     msgsChannels[2],
					FreqRatio: 1,
				},
			},
			Priority: 10,
		},
		{
			QueuesWithFreqRatios: []pq.QueueFreqRatio[*Msg]{
				{
					QueueName: "Priority-1000",
					MsgsC:     msgsChannels[3],
					FreqRatio: 1,
				},
			},
			Priority: 1000,
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

	go pq.ProcessMessagesByPriorityAmongFreqRatioQueueGroups(ctx, queues, msgProcessor)

	time.Sleep(3 * time.Second)
	cancel()

	expectedResults := []*Msg{
		{Body: "Priority-1000 Msg-1"},
		{Body: "Priority-10 Msg-1"},
		{Body: "Priority-10 Msg-2"},
		{Body: "Priority-10 Msg-3"},
		{Body: "Priority-10 Msg-4"},
		{Body: "Priority-10 Msg-5"},
		{Body: "Priority-10 Msg-6"},
		{Body: "Priority-10 Msg-7"},
		{Body: "Priority-10 Msg-8"},
		{Body: "Priority-10 Msg-9"},
		{Body: "Priority-10 Msg-10"},
		{Body: "Priority-10 Msg-11"},
		{Body: "Priority-10 Msg-12"},
		{Body: "Priority-10 Msg-13"},
		{Body: "Priority-10 Msg-14"},
		{Body: "Priority-10 Msg-15"},
		{Body: "Priority-5 Msg-1"},
		{Body: "Priority-5 Msg-2"},
		{Body: "Priority-5 Msg-3"},
		{Body: "Priority-5 Msg-4"},
		{Body: "Priority-5 Msg-5"},
		{Body: "Priority-1 Msg-1"},
		{Body: "Priority-5 Msg-6"},
		{Body: "Priority-5 Msg-7"},
		{Body: "Priority-5 Msg-8"},
		{Body: "Priority-5 Msg-9"},
		{Body: "Priority-5 Msg-10"},
		{Body: "Priority-1 Msg-2"},
		{Body: "Priority-5 Msg-11"},
		{Body: "Priority-5 Msg-12"},
		{Body: "Priority-5 Msg-13"},
		{Body: "Priority-5 Msg-14"},
		{Body: "Priority-5 Msg-15"},
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

func TestProcessMessagesByPriorityAmongFreqRatioQueueGroups_MessagesInOneOfTheChannelsArriveAfterSomeTime(t *testing.T) {
	msgsChannels := make([]chan *Msg, 3)
	msgsChannels[0] = make(chan *Msg, 7)
	msgsChannels[1] = make(chan *Msg, 7)
	msgsChannels[2] = make(chan *Msg, 7)

	queues := []pq.PriorityQueueGroupWithFreqRatio[*Msg]{
		{
			QueuesWithFreqRatios: []pq.QueueFreqRatio[*Msg]{
				{
					QueueName: "Priority-1",
					MsgsC:     msgsChannels[0],
					FreqRatio: 1,
				},
				{
					QueueName: "Priority-2",
					MsgsC:     msgsChannels[1],
					FreqRatio: 2,
				},
			},
			Priority: 1,
		},
		{
			QueuesWithFreqRatios: []pq.QueueFreqRatio[*Msg]{
				{
					QueueName: "Priority-3",
					MsgsC:     msgsChannels[2],
					FreqRatio: 1,
				},
			},
			Priority: 2,
		},
	}

	simulateLongProcessingMsg := "Simulate long processing"
	for j := 1; j <= 5; j++ {
		msgsChannels[0] <- &Msg{Body: fmt.Sprintf("Priority-1 Msg-%d", j)}
		suffix := ""
		if j == 5 {
			suffix = " - " + simulateLongProcessingMsg
		}
		msgsChannels[2] <- &Msg{Body: fmt.Sprintf("Priority-3 Msg-%d%s", j, suffix)}
	}

	waitForMessagesFromPriority2Chan := make(chan struct{})
	var results []*Msg
	msgProcessor := func(_ context.Context, msg *Msg, queueName string) {
		if strings.HasSuffix(msg.Body, simulateLongProcessingMsg) {
			<-waitForMessagesFromPriority2Chan
		}
		results = append(results, msg)
	}
	ctx, cancel := context.WithCancel(context.Background())

	go pq.ProcessMessagesByPriorityAmongFreqRatioQueueGroups(ctx, queues, msgProcessor)

	time.Sleep(1 * time.Second)
	for j := 6; j <= 7; j++ {
		msgsChannels[0] <- &Msg{Body: fmt.Sprintf("Priority-1 Msg-%d", j)}
		msgsChannels[2] <- &Msg{Body: fmt.Sprintf("Priority-3 Msg-%d", j)}
	}
	for j := 1; j <= 7; j++ {
		msgsChannels[1] <- &Msg{Body: fmt.Sprintf("Priority-2 Msg-%d", j)}
	}
	waitForMessagesFromPriority2Chan <- struct{}{}

	time.Sleep(3 * time.Second)
	cancel()

	expectedResults := []*Msg{
		{Body: "Priority-3 Msg-1"},
		{Body: "Priority-3 Msg-2"},
		{Body: "Priority-3 Msg-3"},
		{Body: "Priority-3 Msg-4"},
		{Body: "Priority-3 Msg-5 - Simulate long processing"},
		{Body: "Priority-3 Msg-6"},
		{Body: "Priority-3 Msg-7"},
		{Body: "Priority-1 Msg-1"},
		{Body: "Priority-2 Msg-1"},
		{Body: "Priority-2 Msg-2"},
		{Body: "Priority-2 Msg-3"},
		{Body: "Priority-2 Msg-4"},
		{Body: "Priority-1 Msg-2"},
		{Body: "Priority-2 Msg-5"},
		{Body: "Priority-2 Msg-6"},
		{Body: "Priority-1 Msg-3"},
		{Body: "Priority-2 Msg-7"},
		{Body: "Priority-1 Msg-4"},
		{Body: "Priority-1 Msg-5"},
		{Body: "Priority-1 Msg-6"},
		{Body: "Priority-1 Msg-7"},
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
