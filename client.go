package rediver

import (
	"encoding/json"
	"fmt"
	"github.com/mediocregopher/radix.v3"
	"strings"
	"sync"
	"time"
)

type Client struct {
	id  string
	env string

	redisC   radix.Client
	receiver radix.PubSubConn

	msgMap      sync.Map
	sendChan    chan *Msg
	receiveChan chan radix.PubSubMessage
}

func NewClient(env, redisAddr string) *Client {
	c := &Client{
		id:  RandString(8, ""),
		env: env,

		redisC:      dialRedis(redisAddr),
		sendChan:    make(chan *Msg),
		receiveChan: make(chan radix.PubSubMessage),
		receiver:    radix.PersistentPubSub("tcp", redisAddr, nil),
	}

	if err := c.receiver.PSubscribe(c.receiveChan, c.id+".*"); err != nil {
		panic(err)
	}

	go c.receive()
	go c.send()

	return c
}

func (c *Client) send() {
	for {
		m := <-c.sendChan
		if m == nil {
			break
		}

		b, err := json.Marshal(m)
		if err != nil {
			continue
		}
		key := c.env + ":" + m.Subject[:strings.Index(m.Subject, ".")]
		c.redisC.Do(radix.Cmd(nil, "LPUSH", key, string(b)))
	}
}

func (c *Client) receive() {
	for {
		msg := <-c.receiveChan
		msgch, ok := c.msgMap.Load(msg.Channel)
		if !ok {
			fmt.Println("no func found by:", msg.Channel)
			continue
		}
		msgch.(chan []byte) <- msg.Message
	}
}

func (c *Client) Request(path string, data map[string]interface{}, timeout time.Duration, headerAndCookie ...map[string]interface{}) ([]byte, error) {

	reply := c.id + "." + RandString(32, "")
	msg := &Msg{
		Data:    data,
		Subject: path,
		Reply:   reply,
	}

	h := map[string]interface{}{
		"Request-Time": time.Now().UnixNano(),
		"Timeout":      timeout,
	}

	if len(headerAndCookie) > 0 {
		for k, v := range headerAndCookie[0] {
			h[k] = v
		}
	}
	msg.Header = h

	if len(headerAndCookie) > 1 {
		msg.Cookie = headerAndCookie[1]
	}

	ch := make(chan []byte, 1)
	c.msgMap.Store(reply, ch)
	defer c.msgMap.Delete(reply)

	ticker := time.NewTicker(timeout)
	defer ticker.Stop()

	c.sendChan <- msg

	select {
	case <-ticker.C:
		return nil, fmt.Errorf("timeout")
	case res := <-ch:
		return res, nil
	}

	return nil, fmt.Errorf("receive response error")
}
