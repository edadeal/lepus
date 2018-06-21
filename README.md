# lepus

[![GoDoc](https://godoc.org/github.com/edadeal/lepus?status.svg)](https://godoc.org/github.com/edadeal/lepus)
[![Go Report Card](https://goreportcard.com/badge/github.com/edadeal/lepus)](https://goreportcard.com/report/github.com/edadeal/lepus)

Simple wrapper around [streadway/amqp](https://github.com/streadway/amqp) with syncronous functions.

## Installation

Install:

```shell
go get -u github.com/edadeal/lepus
```

Import:

```go
import "github.com/edadeal/lepus"
```

## Quickstart

```go
func main() {
	conn, err := amqp.Dial("amqp://lepus:lepus@127.0.0.1:5672/lepus")
	if err != nil {
		log.Fatal(err)
    }
    
    defer conn.Close()

    ch, err := lepus.SyncChannel(conn.Channel())
	if err != nil {
		t.Fatal(err)
    }
    
    _, err = ch.QueueDeclare(
		"test", // name
		true,   // durable
		false,  // delete when unused
		false,  // exclusive
		false,  // no-wait
		nil,    // arguments
    )
    if err != nil {
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
    
    log.Printf("Published: %t", state == lepus.StatePublished)
}
```
