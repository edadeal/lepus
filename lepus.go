package lepus

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
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
			wd <- Delivery{msg, 0}
		}
	}()

	return wd, nil
}

// Delivery is a superset of amqp.Delivery
type Delivery struct {
	amqp.Delivery
	acked int32
}

// Nack is a concurrent safe wrapper around standart AMQP Nack
func (d *Delivery) Nack(multiple, requeue bool) error {
	if atomic.CompareAndSwapInt32(&d.acked, 0, 1) {
		err := d.Delivery.Nack(multiple, requeue)
		if err != nil {
			atomic.StoreInt32(&d.acked, 0)
		}
		return err
	}
	return nil
}

// Ack is a concurrent safe wrapper around standart AMQP Ack
func (d *Delivery) Ack(multiple bool) error {
	if atomic.CompareAndSwapInt32(&d.acked, 0, 1) {
		err := d.Delivery.Ack(multiple)
		if err != nil {
			atomic.StoreInt32(&d.acked, 0)
		}
		return err
	}
	return nil
}

// Reject is a concurrent safe wrapper around standart AMQP Reject
func (d *Delivery) Reject(requeue bool) error {
	if atomic.CompareAndSwapInt32(&d.acked, 0, 1) {
		err := d.Delivery.Reject(requeue)
		if err != nil {
			atomic.StoreInt32(&d.acked, 0)
		}
		return err
	}
	return nil
}

// NackDelayed nacks message without requeue and publishes it again
// without modification back to tail of queue
func (d *Delivery) NackDelayed(multiple, mandatory, immediate bool) (State, error) {
	swapped := atomic.CompareAndSwapInt32(&d.acked, 0, 1)
	if !swapped {
		return StateAlreadyProcessed, nil
	}

	ch, ok := d.Delivery.Acknowledger.(*Channel)
	if !ok {
		atomic.StoreInt32(&d.acked, 0)
		return StateUnknown, errors.New("Acknowledger is not of type *lepus.Channel")
	}

	err := d.Nack(multiple, false)
	if err != nil {
		atomic.StoreInt32(&d.acked, 0)
		return StateUnknown, err
	}

	state, err := ch.PublishAndWait(d.Delivery.Exchange, d.Delivery.RoutingKey, mandatory, immediate, amqp.Publishing{
		Headers:         d.Delivery.Headers,
		ContentType:     d.Delivery.ContentType,
		ContentEncoding: d.Delivery.ContentEncoding,
		DeliveryMode:    d.Delivery.DeliveryMode,
		Priority:        d.Delivery.Priority,
		CorrelationId:   d.Delivery.CorrelationId,
		ReplyTo:         d.Delivery.ReplyTo,
		Expiration:      d.Delivery.Expiration,
		MessageId:       d.Delivery.MessageId,
		Timestamp:       d.Delivery.Timestamp,
		Type:            d.Delivery.Type,
		UserId:          d.Delivery.UserId,
		AppId:           d.Delivery.AppId,
		Body:            d.Delivery.Body,
	})

	if err != nil {
		atomic.StoreInt32(&d.acked, 0)
		return StateUnknown, err
	}

	return state, nil
}

// MustPublish can be used as a wrapper around `PublishAndWait` and
// `NackDelayed` methods if you didn't want to process error and state
// separately.
func MustPublish(s State, err error) error {
	if err != nil {
		return err
	}
	if s != StatePublished {
		return fmt.Errorf("Message publishing failed. Result state: %s", s)
	}
	return nil
}
