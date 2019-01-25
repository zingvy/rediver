package rediver

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"sync"
	"time"
)

type Client struct {
	id  string

	redisC   *redis.Client
	msgMap      sync.Map
	sendChan    chan *Msg
}

func NewClient(redisAddr string) *Client {
	c := &Client{
		id:  RandString(8, ""),
		redisC:      dialRedis(redisAddr),
		sendChan:    make(chan *Msg),
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

		msgch, ok := c.msgMap.Load(m.Reply)
		if !ok {
			msgch.(chan []byte) <- []byte("invalid request")
			continue
		}
		b, err := json.Marshal(m)
		if err != nil {
			msgch.(chan []byte) <- []byte("invalid request")
			continue
		}

		sub, err := parseSubject(m.Subject)
		if err != nil {
			msgch.(chan []byte) <- []byte("invalid request")
			continue
			
		}
		key := sub.Env+":"+sub.App

		c.redisC.LPush(key, b)
	}
}

func (c *Client) receive() {
	pubsub := c.redisC.PSubscribe(c.id+".*")
	defer pubsub.Close()

	ch := pubsub.Channel()
	for msg := range ch {
		msgch, ok := c.msgMap.Load(msg.Channel)
		if !ok {
			fmt.Println("no func found by:", msg.Channel)
			continue
		}
		msgch.(chan []byte) <- []byte(msg.Payload)
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
