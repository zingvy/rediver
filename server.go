package rediver

import (
	"encoding/json"
	"github.com/go-redis/redis"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"log"
)

const (
	DefaultPoolSize = 5
)

type Handler interface{}
type MiddleFunc func(*ReqContext, func())

type Rediver struct {
	id       string
	handlers map[string]Handler
	middles  []MiddleFunc
	env      string
	app      string

	readPool *redis.Client

	replyPool *redis.Client
	replyChan chan *ReqContext

	wg      sync.WaitGroup
	running int32

	closeChan chan bool
	reqChan   chan []byte
}

func dialRedis(redisAddr ...string) *redis.Client {
	var c *redis.Client
	if len(redisAddr) == 1 {
		c = redis.NewClient(&redis.Options{
    		Addr:     redisAddr[0],
			MinIdleConns: DefaultPoolSize,
			MaxConnAge: 0,
		})
	} else {
		c = redis.NewFailoverClient(&redis.FailoverOptions{
    		MasterName:    "master", //TODO
    		SentinelAddrs: redisAddr,
		})
	}
	_, err := c.Ping().Result()
	if err != nil {
		panic("ping to redis error: " + err.Error())
	}
	return c
}

func New(env, app, id, redisAddr string) *Rediver {
	s := &Rediver{
		id:        id,
		handlers:  map[string]Handler{},
		middles:   make([]MiddleFunc, 0),
		env:       env,
		app:       app,
		readPool:  dialRedis(redisAddr),
		replyPool: dialRedis(redisAddr),
		replyChan: make(chan *ReqContext),

		closeChan: make(chan bool, 1),
		reqChan:   make(chan []byte),
	}
	return s
}

func (s *Rediver) sig() {
	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGTERM, syscall.SIGINT)
	si := <-sigC
	log.Println("received signal:", si)
	log.Printf("there are %d task running\n", s.Running())
	s.closeChan <- true
}

func (s *Rediver) reply() {
	for {
		r := <-s.replyChan
		if r == nil {
			break
		}

		if r.Resp == nil || r.reply == "" {
			continue
		}

		jsonByte, err := json.Marshal(r.Resp)
		if err != nil {
			continue
		}
		s.replyPool.Publish(r.reply, jsonByte)
	}
}

func (s *Rediver) startOne() {
	s.wg.Add(1)
	atomic.AddInt32(&s.running, 1)
}

func (s *Rediver) finishOne() {
	atomic.AddInt32(&s.running, -1)
	s.wg.Done()
}

func (s *Rediver) Running() int32 {
	return atomic.LoadInt32(&s.running)
}

func (s *Rediver) Route(pattern string, h Handler) {
	s.handlers[pattern] = h
}

func (s *Rediver) Use(funcs ...MiddleFunc) {
	for _, f := range funcs {
		s.middles = append(s.middles, f)
	}
}

func (s *Rediver) pop() {
	queue := s.env + ":" + s.app
	go func() {
		<- s.closeChan
		s.readPool.Close()
	}()

	for {
		result, err := s.readPool.BLPop(0, queue).Result()
		if err != nil {
			log.Println("error when getting task: ", err)
			s.reqChan <- nil
		} else {
			s.reqChan <- []byte(result[1])	
		}
	}
}

func (s *Rediver) Serving() {
	go s.sig()
	go s.reply()
	go s.pop()

	for {
		v := <-s.reqChan
		if v == nil {
			break
		}
		go func(d []byte) {
			s.startOne()
			defer s.finishOne()
			s.dispatcher(d)
		}(v)
	}
	s.wg.Wait()
	s.replyChan <- nil
	s.readPool.Close()
	s.replyPool.Close()
}

/////////////////////////
//   request handle    //
/////////////////////////

type Subject struct {
	Env    string
	App    string
	Module string
	Method string
}

func parseSubject(subj string) (*Subject, error) {
	n := strings.Split(subj, ".")
	if len(n) < 3 {
		return nil, fmt.Errorf("invalid subject")
	}
	env := n[0]
	app := n[1]
	module := strings.Join(n[2:len(n)-1], ".")
	method := n[len(n)-1]
	return &Subject{env, app, module, method}, nil
}

func (s *Rediver) dispatcher(data []byte) {
	var rc *ReqContext
	var msg Msg

	defer func() {
		if rc != nil {
			s.replyChan <- rc
		}
	}()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("SERVER INTERNAL ERROR: <path:%s>, <id:%s>\n", msg.Subject, msg.Reply)
			if rc != nil {
				rc.Error(500, nil, "SERVER INTERNAL ERROR: "+r.(string))
			}
		}
	}()

	err := json.Unmarshal(data, &msg)
	if err != nil {
		return
	}
	if msg.Reply == "" {
		log.Printf("request should specify reply channel: <path:%s>, <id:%s>\n", msg.Subject, msg.Reply)
		return
	}

	start, _ := msg.Header.GetInt("Request-Time")
	timeout, _ := msg.Header.GetInt("Timeout")
	if timeout != 0 && start+timeout < time.Now().UnixNano() {
		log.Printf("request timeout, abort: <path:%s>, <id:%s>\n", msg.Subject, msg.Reply)
		return
	}

	subj, err := parseSubject(msg.Subject)
	if err != nil {
		log.Printf("invalid request, abort: <path:%s>, <id:%s>\n", msg.Subject, msg.Reply)
		return

	}
	rc = &ReqContext{
		Module:    subj.Module,
		Method:    subj.Method,
		reply:     msg.Reply,
		server:    s,
		Param:     msg.Data,
		Header:    msg.Header,
		Cookie:    msg.Cookie,
		StartTime: time.Unix(0, start),
	}

	i := 0
	var next func()
	next = func() {
		if i < len(s.middles) {
			i++
			s.middles[i-1](rc, next)
		} else {
			s.do(rc)
		}
	}
	next()
}

func (s *Rediver) do(rc *ReqContext) {
	h, ok := s.handlers[rc.Module]
	if !ok {
		rc.Error(404, nil, "module not found: "+rc.Module)
		return
	}
	action := reflect.ValueOf(h).MethodByName(strings.Title(rc.Method))
	if !action.IsValid() {
		rc.Error(404, nil, "method not found: "+rc.Method)
		return
	}

	args := []reflect.Value{reflect.ValueOf(rc)}
	action.Call(args)
}
