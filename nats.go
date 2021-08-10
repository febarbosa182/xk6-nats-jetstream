package nats

import (
	"context"

	"time"
	"strings"

	"github.com/loadimpact/k6/js/common"
	"github.com/nats-io/nats.go"

	"github.com/loadimpact/k6/js/modules"
)

func init() {
	modules.Register("k6/x/nats", new(Nats))
}

type Nats struct{}

type Client struct {
	client         *nats.Conn
	defaultTimeout time.Duration
}

func (r *Nats) XClient(ctxPtr *context.Context, natsServer string, timeoutMS int) interface{} {
	rt := common.GetRuntime(*ctxPtr)

	conn, err := nats.Connect(natsServer)
	if err != nil {
		panic(err)
	}
	var timeout time.Duration
	if timeoutMS == 0 {
		timeout = time.Millisecond * 5 // 5 second default timeout
	} else {
		timeout = time.Millisecond * time.Duration(timeoutMS)
	}
	return common.Bind(rt, &Client{client: conn, defaultTimeout: timeout}, ctxPtr)
}

func (c *Client) Request(subject string, payload string) (string, error) {
	msg, err := c.client.Request(subject, []byte(payload), c.defaultTimeout)
	if err != nil {
		return "", err
	}
	return string(msg.Data), err
}

func (c *Client) Publish(subject string, payload string) error {
	return c.client.Publish(subject, []byte(payload))
}

func (c *Client) PublishJetstream(stream string, subject string, payload string, ackwait int) error {
	// return c.client.Publish(subject, []byte(payload))
	js, err := c.client.JetStream()
	if err != nil {
		return "", err
	}

	// Set custom timeout for a JetStream API request.
	js.AddStream(&nats.StreamConfig{
		Name:     strings.ToTitle(stream),
		Subjects: []string{subject},
	})

	// Wait for an ack response .
	return js.Publish(subject, []byte(payload), nats.AckWait(ackwait))
}
