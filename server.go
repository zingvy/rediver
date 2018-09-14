package rediver

import (
	"encoding/json"
	"github.com/mediocregopher/radix.v3"
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

	redisPool radix.Client
	replyChan chan *ReqContext

	wg      sync.WaitGroup
	running int32

	closeChan chan bool
	reqChan   chan []byte
}

func dialRedis(redisAddr ...string) radix.Client {
	var c radix.Client

	if len(redisAddr) == 1 {
		c, err := radix.NewPool("tcp", redisAddr[0], DefaultPoolSize)
		if err != nil {
			log.Panic(err)
		}
		return c
	} else {
		// TODO  with sentinel or cluster
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
		redisPool: dialRedis(redisAddr),
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
		s.redisPool.Do(radix.Cmd(nil, "PUBLISH", r.reply, string(jsonByte)))
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
	s.redisPool.Do(radix.WithConn(queue, func(conn radix.Conn) error {
		go func(c radix.Conn) {
			<-s.closeChan
			c.Close()
		}(conn)

		for {
			var v [][]byte
			if err := conn.Do(radix.Cmd(&v, "BRPOP", queue, "0")); err != nil {
				log.Println("error when getting task: ", err)
				s.reqChan <- nil
				return err
			} else {
				s.reqChan <- v[1]
			}
		}
		return nil
	}))
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
	s.redisPool.Close()
}

/////////////////////////
//   request handle    //
/////////////////////////

type Subject struct {
	App    string
	Module string
	Method string
}

func parseSubject(subj string) *Subject {
	n := strings.Split(subj, ".")
	if len(n) < 2 {
		log.Panic("invalid subject: " + subj)
	}
	app := n[0]
	module := strings.Join(n[1:len(n)-1], ".")
	method := n[len(n)-1]
	return &Subject{app, module, method}
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

	subj := parseSubject(msg.Subject)
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
