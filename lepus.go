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

	sm  map[string]*info
	smu sync.Mutex

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
		sm:        make(map[string]*info),
		smu:       &sync.Mutex{},
		timeout:   2 * time.Second,
	}

	c.pubc = ch.NotifyPublish(make(chan amqp.Confirmation))
	go func() {
		for pub := range c.pubc {
			smkey := "DeliveryTag-" + strconv.Itoa(int(pub.DeliveryTag))
			if inf, ok := c.sm[smkey]; ok {
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
			if inf, ok := c.sm[smkey]; ok {
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
		for _, inf := range c.sm {
			swapped := atomic.CompareAndSwapInt32(&inf.state, int32(StateUnknown), int32(StateClosed))
			if swapped {
				inf.err = err
				inf.mu.Unlock()
			}
		}
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

	c.smu.Lock()
	c.sm[mkey] = inf
	c.sm[tkey] = inf
	c.smu.Unlock()

	defer func() {
		c.smu.Lock()
		delete(c.sm, mkey)
		delete(c.sm, tkey)
		c.smu.Unlock()
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
