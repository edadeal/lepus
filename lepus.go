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
	mu    sync.Mutex
	state int32
	err   error
}

// Channel is a wrapper around async AMQP channel
type Channel struct {
	*amqp.Channel

	messageCount uint64
	midPrefix    string

	pubc  chan amqp.Confirmation
	retc  chan amqp.Return
	closc chan *amqp.Error

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

	c.pubc = ch.NotifyPublish(make(chan amqp.Confirmation))
	go func() {
		for pub := range c.pubc {
			smkey := "DeliveryTag-" + strconv.Itoa(int(pub.DeliveryTag))
			if iinf, ok := c.sm.Load(smkey); ok {
				inf := iinf.(*info)
				swapped := atomic.CompareAndSwapInt32(&inf.state, int32(StateUnknown), int32(StatePublished))
				if swapped {
					inf.mu.Unlock()
				}
			}
		}
	}()

	c.retc = ch.NotifyReturn(make(chan amqp.Return))
	go func() {
		for ret := range c.retc {
			smkey := ret.MessageId + ret.CorrelationId
			if iinf, ok := c.sm.Load(smkey); ok {
				inf := iinf.(*info)
				swapped := atomic.CompareAndSwapInt32(&inf.state, int32(StateUnknown), int32(StateReturned))
				if swapped {
					inf.mu.Unlock()
				}
			}
		}
	}()

	c.closc = ch.NotifyClose(make(chan *amqp.Error))
	go func() {
		err := <-c.closc
		c.sm.Range(func(key, value interface{}) bool {
			inf := value.(*info)
			swapped := atomic.CompareAndSwapInt32(&inf.state, int32(StateUnknown), int32(StateClosed))
			if swapped {
				inf.err = err
				inf.mu.Unlock()
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

	inf := &info{
		state: int32(StateUnknown),
		mu:    sync.Mutex{},
	}
	inf.mu.Lock()

	mkey := msg.MessageId + msg.CorrelationId
	tkey := "DeliveryTag-" + strconv.Itoa(int(mid))

	c.sm.Store(mkey, inf)
	c.sm.Store(tkey, inf)

	defer func() {
		c.sm.Delete(mkey)
		c.sm.Delete(tkey)
	}()

	err := c.Publish(exchange, key, mandatory, immediate, msg)
	if err != nil {
		return StateUnknown, err
	}

	go func() {
		timer := time.NewTimer(c.timeout)
		defer timer.Stop()

		<-timer.C
		swapped := atomic.CompareAndSwapInt32(&inf.state, int32(StateUnknown), int32(StateTimeout))
		if swapped {
			inf.err = errors.New("message publishing timeout reached")
			inf.mu.Unlock()
		}
	}()

	inf.mu.Lock()
	return State(inf.state), inf.err
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
			wd <- Delivery{d: msg}
		}
	}()

	return wd, nil
}

// Delivery is a superset of amqp.Delivery
type Delivery struct {
	d     amqp.Delivery
	acked int32
}

// Nack is a concurrent safe wrapper around standart AMQP Nack
func (d *Delivery) Nack(multiple, requeue bool) error {
	if atomic.CompareAndSwapInt32(&d.acked, 0, 1) {
		err := d.d.Nack(multiple, requeue)
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
		err := d.d.Ack(multiple)
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
		err := d.d.Reject(requeue)
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
	ch, ok := d.d.Acknowledger.(*Channel)
	if !ok {
		return StateUnknown, errors.New("Acknowledger is not of type *lepus.Channel")
	}

	err := d.Nack(multiple, false)
	if err != nil {
		return StateUnknown, err
	}

	return ch.PublishAndWait(d.d.Exchange, d.d.RoutingKey, mandatory, immediate, amqp.Publishing{
		Headers:         d.d.Headers,
		ContentType:     d.d.ContentType,
		ContentEncoding: d.d.ContentEncoding,
		DeliveryMode:    d.d.DeliveryMode,
		Priority:        d.d.Priority,
		CorrelationId:   d.d.CorrelationId,
		ReplyTo:         d.d.ReplyTo,
		Expiration:      d.d.Expiration,
		MessageId:       d.d.MessageId,
		Timestamp:       d.d.Timestamp,
		Type:            d.d.Type,
		UserId:          d.d.UserId,
		AppId:           d.d.AppId,
		Body:            d.d.Body,
	})
}
