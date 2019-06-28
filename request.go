package rediver

import (
	"fmt"
	"net/http"
	"strconv"
	"time"
)

type Msg struct {
	Subject string `json:"subject"`
	Data    Params `json:"data"`
	Cookie  Params `json:"cookie,omitempty"`
	Header  Params `json:"header,omitempty"`
	Reply   string `json:"reply"`
}

type ReqContext struct {
	Param     Params
	Header    Params
	Cookie    Params
	Module    string
	Method    string
	Resp      *Response
	StartTime time.Time

	reply      string
	server     *Rediver
	extLogs    []string
	newCookies []*http.Cookie
}
type Response struct {
	Code   int            `json:"code"`
	Errmsg string         `json:"errmsg"`
	Result interface{}    `json:"result"`
	Cookie []*http.Cookie `json:"COOKIE,omitempty"`
	Header map[string]string `json:"HEADER,omitempty"`
}

type Params map[string]interface{}

func (p Params) Has(key string) bool {
	_, ok := p[key]
	return ok
}

// GetString or panic
func (p Params) GetWithError(key string) (string, error)  {
	v, ok := p[key]
	if !ok {
		return "", fmt.Errorf(key)
	}
	return v.(string), nil
}

// by default return string
func (p Params) Get(key string) string {
	return p.GetString(key)
}

func (p Params) GetString(key string) string {
	if v, ok := p[key]; ok {
		return v.(string)
	}
	return ""
}

func (p Params) GetInt(key string) (int64, error) {
	if v, ok := p[key]; ok {
		switch v.(type) {
		case string:
			return strconv.ParseInt(v.(string), 10, 64)
		case int64, int:
			return v.(int64), nil
		case float64:
			return int64(v.(float64)), nil
		}
	}
	return 0, fmt.Errorf("key:%s not found", key)
}

func NewResponse(code int, result interface{}) *Response {
	res := new(Response)
	res.Code = code
	res.Result = result
	res.Header = map[string]string{"Content-Type": "application/json"}
	return res
}

func (rc *ReqContext) ID() string {
	return rc.reply
}

func (rc *ReqContext) ExtLogs() []string {
	return rc.extLogs
}

func (rc *ReqContext) ServerID() string {
	return rc.server.id
}

func (rc *ReqContext) App() string {
	return rc.server.app
}

func (rc *ReqContext) AppendExtLog(l string) {
	rc.extLogs = append(rc.extLogs, l)
}

func (rc *ReqContext) SetCookie(c *http.Cookie) {
	rc.newCookies = append(rc.newCookies, c)
}

func (rc *ReqContext) Error(code int, result interface{}, errorMsg string) {
	res := NewResponse(code, result)
	res.Errmsg = errorMsg
	if len(rc.newCookies) == 0 {
		res.Cookie = rc.newCookies
	}
	rc.Resp = res
}

func (rc *ReqContext) Json(result interface{}) {
	res := NewResponse(0, result)
	if len(rc.newCookies) > 0 {
		res.Cookie = rc.newCookies
	}
	rc.Resp = res
}

func (rc *ReqContext) Text(result string) {
	res := NewResponse(0, result)
	res.Header["Content-Type"] = "text/plain"
	if len(rc.newCookies) > 0 {
		res.Cookie = rc.newCookies
	}
	rc.Resp = res
}

func (rc *ReqContext) Image(result []byte) {
	res := NewResponse(0, result)
	res.Header["Content-Type"] = "image/jpg"
	if len(rc.newCookies) > 0 {
		res.Cookie = rc.newCookies
	}
	rc.Resp = res
}
