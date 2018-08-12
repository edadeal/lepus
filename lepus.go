package lepus

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
)

// State indicates publishing state of message
type State int32

// states
const (
	StateUnknown State = iota
	StatePublished
	StateReturned
	StateTimeout
	StateClosed
)

type info struct {
	state int32
	err   error
}

// Channel is a wrapper around async AMQP channel
type Channel struct {
	*amqp.Channel

	messageCount uint64
	midPrefix    string

	sm sync.Map

	timeout time.Duration
}

// SyncChannel returns channel wrapper
func SyncChannel(ch *amqp.Channel, err error) (*Channel, error) {
	if err != nil {
		return nil, err
	}

	if err := ch.Confirm(false); err != nil {
		return nil, err
	}

	c := &Channel{
		Channel:   ch,
		midPrefix: "lepus-" + strconv.Itoa(int(time.Now().Unix())),
		timeout:   2 * time.Second,
	}

	go func() {
		pubc := ch.NotifyPublish(make(chan amqp.Confirmation))
		for pub := range pubc {
			smkey := "DeliveryTag-" + strconv.Itoa(int(pub.DeliveryTag))
			if v, ok := c.sm.Load(smkey); ok {
				v.(chan info) <- info{
					state: int32(StatePublished),
				}
			}
		}
	}()

	go func() {
		retc := ch.NotifyReturn(make(chan amqp.Return))
		for ret := range retc {
			smkey := ret.MessageId + ret.CorrelationId
			if v, ok := c.sm.Load(smkey); ok {
				v.(chan info) <- info{
					state: int32(StateReturned),
				}
			}
		}
	}()

	go func() {
		err := <-ch.NotifyClose(make(chan *amqp.Error))
		c.sm.Range(func(k, v interface{}) bool {
			v.(chan info) <- info{
				state: int32(StateClosed),
				err:   err,
			}
			return true
		})
	}()

	return c, nil
}

// WithTimeout sets publish wait timeout
func (c *Channel) WithTimeout(d time.Duration) {
	c.timeout = d
}

// PublishAndWait sends message to queue and waits for response
func (c *Channel) PublishAndWait(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (State, error) {
	mid := atomic.AddUint64(&c.messageCount, 1)
	if msg.MessageId == "" {
		msg.MessageId = c.midPrefix + "-" + strconv.Itoa(int(mid))
	}

	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	if msg.CorrelationId == "" {
		msg.CorrelationId = strconv.Itoa(int(mid))
	}

	mkey := msg.MessageId + msg.CorrelationId
	tkey := "DeliveryTag-" + strconv.Itoa(int(mid))

	sch := make(chan info)

	c.sm.Store(mkey, sch)
	c.sm.Store(tkey, sch)

	defer func() {
		c.sm.Delete(mkey)
		c.sm.Delete(tkey)
	}()

	err := c.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		return StateUnknown, err
	}

	timer := time.NewTimer(c.timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return StateTimeout, errors.New("message publishing timeout reached")
		case inf := <-sch:
			return State(inf.state), inf.err
		}
	}
}

// ConsumeMessages returns chan of wrapped messages from queue
func (c *Channel) ConsumeMessages(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan Delivery, error) {
	d, err := c.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		return nil, err
	}

	wd := make(chan Delivery)
	go func() {
		for msg := range d {
			msg.Acknowledger = c
			wd <- Delivery{msg}
		}
	}()

	return wd, nil
}

// Delivery is a superset of amqp.Delivery
type Delivery struct {
	amqp.Delivery
}

// NackDelayed nacks message without requeue and publishes it again
// without modification back to tail of queue
func (d *Delivery) NackDelayed(multiple, mandatory, immediate bool) (State, error) {
	ch, ok := d.Acknowledger.(*Channel)
	if !ok {
		return StateUnknown, errors.New("Acknowledger is not of type *lepus.Channel")
	}

	err := d.Nack(multiple, false)
	if err != nil {
		return StateUnknown, err
	}

	return ch.PublishAndWait(d.Exchange, d.RoutingKey, mandatory, immediate, amqp.Publishing{
		Headers:         d.Headers,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		DeliveryMode:    d.DeliveryMode,
		Priority:        d.Priority,
		CorrelationId:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Expiration:      d.Expiration,
		MessageId:       d.MessageId,
		Timestamp:       d.Timestamp,
		Type:            d.Type,
		UserId:          d.UserId,
		AppId:           d.AppId,
		Body:            d.Body,
	})
}
