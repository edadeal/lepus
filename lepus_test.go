package lepus

import (
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

var testConn *amqp.Connection

func declareTestQueue(ch *Channel) error {
	_, err := ch.QueueDeclare(
		"test", // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
	)
	return err
}

func TestMain(m *testing.M) {
	var err error

	testConn, err = amqp.Dial("amqp://lepus:lepus@127.0.0.1:5672/lepus")
	if err != nil {
		log.Fatal(err)
	}

	code := m.Run()
	testConn.Close()

	os.Exit(code)
}

func TestSyncChannel(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("Teardown test")
		time.Sleep(1 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}
}

func TestPublishAndWait(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("Teardown test")
		time.Sleep(1 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}

	state, err := ch.PublishAndWait(
		"",     // exchange
		"test", // routing key
		true,   // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte("Hello, lepus!"),
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	expected := StatePublished
	if state != expected {
		t.Fatalf("Expecting state to be %v got %v", expected, state)
	}
}

func TestPublishAndWaitMultiple(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("Teardown test")
		time.Sleep(1 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		state, err := ch.PublishAndWait(
			"",     // exchange
			"test", // routing key
			true,   // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte("Hello, lepus!"),
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		expected := StatePublished
		if state != expected {
			t.Fatalf("Expecting state to be %v got %v", expected, state)
		}
	}
}

func TestConsumeMessages(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("Teardown test")
		time.Sleep(2 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}

	msgs, err := ch.ConsumeMessages(
		"test", // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		t.Fatal(err)
	}

	messagesCount := 10
	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()

		timeout := time.NewTimer(1 * time.Second)
		defer timeout.Stop()

		var receivedCount int
		for {
			select {
			case msg := <-msgs:
				if err := msg.Ack(false); err != nil {
					t.Fatal(err)
				}
				receivedCount++
			case <-timeout.C:
				log.Println("TestConsumeMessages timeout, stopping consumer")
				goto endloop
			}
		}
	endloop:
		if receivedCount != messagesCount {
			t.Fatalf("Expected received messages count to be %d, got %d", messagesCount, receivedCount)
		}
	}()

	for i := 0; i < messagesCount; i++ {
		state, err := ch.PublishAndWait(
			"",     // exchange
			"test", // routing key
			true,   // mandatory
			false,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "text/plain",
				Body:         []byte("Hello, lepus!"),
			},
		)

		if err != nil {
			t.Fatal(err)
		}

		expected := StatePublished
		if state != expected {
			t.Fatalf("Expecting state to be %v got %v", expected, state)
		}
	}

	wg.Wait()
}

func TestNackDelayed(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("Teardown test")
		time.Sleep(2 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}

	state, err := ch.PublishAndWait(
		"",     // exchange
		"test", // routing key
		true,   // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte("Hello, lepus!"),
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	expected := StatePublished
	if state != expected {
		t.Fatalf("Expecting state to be %v got %v", expected, state)
	}

	msgs, err := ch.ConsumeMessages(
		"test", // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		t.Fatal(err)
	}

	for msg := range msgs {
		state, err := msg.NackDelayed(false, true, false)

		if err != nil {
			t.Fatal(err)
		}

		expected := StatePublished
		if state != expected {
			t.Fatalf("Expecting state to be %v got %v", expected, state)
		}

		return // success
	}
}

func TestPublishTimeout(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	ch.WithTimeout(1 * time.Millisecond)

	defer func() {
		log.Println("Teardown test")
		time.Sleep(2 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}

	state, err := ch.PublishAndWait(
		"",     // exchange
		"test", // routing key
		true,   // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte("Hello, lepus!"),
		},
	)

	if state != StateTimeout {
		t.Fatalf("Expecting state to be %v got %v", StateTimeout, state)
	}

	if err == nil {
		t.Fatal("Expected error, got nil")
	}
}

func TestPublishReturn(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("Teardown test")
		time.Sleep(2 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}

	state, err := ch.PublishAndWait(
		"", // exchange
		"unknown-queue-ololo", // routing key
		true, // mandatory
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte("Hello, lepus!"),
		},
	)

	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	if state != StateReturned {
		t.Fatalf("Expecting state to be %v got %v", StateReturned, state)
	}
}

func TestPublishClose(t *testing.T) {
	ch, err := SyncChannel(testConn.Channel())
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		log.Println("Teardown test")
		time.Sleep(2 * time.Second)
		ch.QueuePurge("test", false)
		ch.Close()
	}()

	if err := declareTestQueue(ch); err != nil {
		t.Fatal(err)
	}

	state, err := ch.PublishAndWait(
		"",     // exchange
		"test", // routing key
		true,   // mandatory
		true,
		amqp.Publishing{
			DeliveryMode: amqp.Transient,
			ContentType:  "text/plain",
			Body:         []byte("Hello, lepus!"),
		},
	)

	if state != StateClosed {
		t.Fatalf("Expecting state to be %v got %v", StateClosed, state)
	}

	if err == nil {
		t.Fatal("Expecting error, got nil")
	}
}
