package nxcore

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/jaracil/ei"
	"github.com/jaracil/smartio"
)

const (
	ErrParse            = -32700
	ErrInvalidRequest   = -32600
	ErrInternal         = -32603
	ErrInvalidParams    = -32602
	ErrMethodNotFound   = -32601
	ErrTtlExpired       = -32011
	ErrPermissionDenied = -32010
	ErrConnClosed       = -32007
	ErrLockNotOwned     = -32006
	ErrUserExists       = -32005
	ErrInvalidUser      = -32004
	ErrInvalidPipe      = -32003
	ErrInvalidTask      = -32002
	ErrCancel           = -32001
	ErrTimeout          = -32000
	ErrNotSupported     = -32099
)

var ErrStr = map[int]string{
	ErrParse:            "Parse error",
	ErrInvalidRequest:   "Invalid request",
	ErrMethodNotFound:   "Method not found",
	ErrInvalidParams:    "Invalid params",
	ErrInternal:         "Internal error",
	ErrTimeout:          "Timeout",
	ErrCancel:           "Cancel",
	ErrInvalidTask:      "Invalid task",
	ErrInvalidPipe:      "Invalid pipe",
	ErrInvalidUser:      "Invalid user",
	ErrUserExists:       "User already exists",
	ErrPermissionDenied: "Permission denied",
	ErrTtlExpired:       "TTL expired",
	ErrLockNotOwned:     "Lock not owned",
	ErrConnClosed:       "Connection is closed",
	ErrNotSupported:     "Not supported",
}

type JsonRpcErr struct {
	Cod  int         `json:"code"`
	Mess string      `json:"message"`
	Dat  interface{} `json:"data,omitempty"`
}

func (e *JsonRpcErr) Error() string {
	return fmt.Sprintf("[%d] %s", e.Cod, e.Mess)
}

func (e *JsonRpcErr) Code() int {
	return e.Cod
}

func (e *JsonRpcErr) Data() interface{} {
	return e.Dat
}

// NewJsonRpcErr creates new JSON-RPC error.
//
// code is the JSON-RPC error code.
// message is optional in case of well known error code (negative values).
// data is an optional extra info object.
func NewJsonRpcErr(code int, message string, data interface{}) error {
	if code < 0 {
		if errstr, ok := ErrStr[code]; ok {
			if message != "" {
				message = fmt.Sprintf("%s:[%s]", errstr, message)
			} else {
				message = errstr
			}
		}
	}

	return &JsonRpcErr{Cod: code, Mess: message, Dat: data}
}

type JsonRpcReq struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      uint64      `json:"id,omitempty"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params"`
	nc      *NexusConn
}

type JsonRpcRes struct {
	Jsonrpc string      `json:"jsonrpc"`
	Id      uint64      `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *JsonRpcErr `json:"error,omitempty"`
}

// NexusConn represents the Nexus connection.
type NexusConn struct {
	connId            string
	conn              net.Conn
	connRx            *smartio.SmartReader
	connTx            *smartio.SmartWriter
	reqCount          uint64
	resTable          map[uint64]chan *JsonRpcRes
	resTableLock      sync.Mutex
	chReq             chan *JsonRpcReq
	closed            int32 //Goroutine safe bool
	context           context.Context
	cancelFun         context.CancelFunc
	wdog              int64
	NexusVersion      *NxVersion
	inactivityTimeout time.Duration
	inactivityTimer   *time.Timer
}

type NxVersion struct {
	Major int
	Minor int
	Patch int
}

func (v *NxVersion) String() string {
	return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Task represents a task pushed to Nexus.
type Task struct {
	nc           *NexusConn
	Id           string                 `json:"id"`
	Stat         string                 `json:"state"`
	Path         string                 `json:"path"`
	Prio         int                    `json:"priority"`
	Ttl          int                    `json:"ttl"`
	Detach       bool                   `json:"detached"`
	User         string                 `json:"user"`
	Method       string                 `json:"method"`
	Params       interface{}            `json:"params"`
	LocalId      interface{}            `json:"-"`
	Tses         string                 `json:"targetSession"`
	Result       interface{}            `json:"result"`
	ErrCode      *int                   `json:"errCode"`
	ErrStr       string                 `json:"errString"`
	ErrObj       interface{}            `json:"errObject"`
	Tags         map[string]interface{} `json:"tags"`
	CreationTime time.Time              `json:"creationTime"`
	DeadLine     time.Time              `json:"deadline"`
}

// TaskOpts represents task push options.
type TaskOpts struct {
	// Task priority default 0 (Set negative value for lower priority)
	Priority int
	// Task ttl default 5
	Ttl int
	// Task detach. If true, task is detached from creating session.
	// If task is detached and creating session deads, task is not removed from tasks queue.
	Detach bool
}

// Pipe represents a pipe.
// Pipes can only be read from the connection that created them and
// can be written from any conection.
type Pipe struct {
	nc        *NexusConn
	pipeId    string
	context   context.Context
	cancelFun context.CancelFunc
}

// Msg represents a pipe single message
type Msg struct {
	Count int64       // Message counter (unique and correlative)
	Msg   interface{} // Pipe message
}

// PipeData represents a pipe messages group obtained in read ops.
type PipeData struct {
	Msgs    []*Msg // Messages
	Waiting int    // Number of messages waiting in Nexus server since last read
	Drops   int    // Number of messages dropped (pipe overflows) since last read
}

// TopicMsg represents a single topic message
type TopicMsg struct {
	Topic string      // Topic the message was published to
	Count int64       // Message counter (unique and correlative)
	Msg   interface{} // The message itself
}

// TopicData represents a topic messages group obtained in read ops.
type TopicData struct {
	Msgs    []*TopicMsg // Messages
	Waiting int         // Number of messages waiting in Nexus server since last read
	Drops   int         // Number of messages dropped (pipe overflows) since last read
}

// PipeOpts represents pipe creation options
type PipeOpts struct {
	Length int // Pipe buffer capacity
}

// ListOpts represents the common options when requesting a list
type ListOpts struct {
	LimitByDepth bool   // Enables the limit of the search to a number of subprefixes
	Depth        int    // If LimitByDepth is true, this depth is used (0 means the exact prefix)
	Filter       string // A RE2 regular expression to filter prefixes on the search
}

// CountOpts represents the common options when requesting a count
type CountOpts struct {
	Subprefixes bool   // Include the count of the subprefixes
	Filter      string // A RE2 regular expression to filter prefixes on the search
}

// NewNexusConn creates new nexus connection from net.conn
func NewNexusConn(conn net.Conn) *NexusConn {
	nc := &NexusConn{
		conn:              conn,
		connRx:            smartio.NewSmartReader(conn),
		connTx:            smartio.NewSmartWriter(conn),
		resTable:          make(map[uint64]chan *JsonRpcRes),
		chReq:             make(chan *JsonRpcReq, 16),
		wdog:              60,
		NexusVersion:      &NxVersion{0, 0, 0},
		inactivityTimeout: -1 * time.Second,
		inactivityTimer:   time.NewTimer(-1 * time.Second),
	}
	//This line is not blocking. By default a negative duration on a timer makes it stop.
	<-nc.inactivityTimer.C

	nc.context, nc.cancelFun = context.WithCancel(context.Background())
	go nc.sendWorker()
	go nc.recvWorker()
	go nc.mainWorker()
	return nc
}

func (nc *NexusConn) pushReq(req *JsonRpcReq) (err error) {
	select {
	case nc.chReq <- req:
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrConnClosed, "", nil)
	}
	return
}

func (nc *NexusConn) pullReq() (req *JsonRpcReq, err error) {
	select {
	case req = <-nc.chReq:
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrConnClosed, "", nil)
	}
	return
}

func (nc *NexusConn) mainWorker() {
	defer nc.Close()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-nc.inactivityTimer.C:
			return
		case <-tick.C:
			now := time.Now().Unix()
			wdog := atomic.LoadInt64(&nc.wdog)
			max := wdog * 2
			if (now-nc.connRx.GetLast() > max) &&
				(now-nc.connTx.GetLast() > max) {
				return
			}
			if (now-nc.connRx.GetLast() >= wdog) &&
				(now-nc.connTx.GetLast() >= wdog) {
				if err := nc.Ping(10 * time.Second); err != nil {
					if time.Now().Unix()-nc.connTx.GetLast() > 5 {
						return
					}
				}
			}
		case <-nc.context.Done():
			return
		}
	}
}

func (nc *NexusConn) sendWorker() {
	defer nc.Close()
	for {
		req, err := nc.pullReq()
		if err != nil {
			break
		}
		req.Jsonrpc = "2.0"
		buf, err := json.Marshal(req)
		if err != nil {
			nc.resTableLock.Lock()
			ch := nc.resTable[req.Id]
			nc.resTableLock.Unlock()
			if ch != nil {
				select {
				case ch <- &JsonRpcRes{Jsonrpc: "2.0", Id: req.Id, Result: nil, Error: &JsonRpcErr{Cod: ErrParse, Mess: ErrStr[ErrParse], Dat: nil}}:
				default:
				}
			}
			continue
		}
		if !nc.Closed() {
			_, err = nc.connTx.Write(buf)
			if err != nil {
				break
			}
		} else {
			break
		}
	}
}

func (nc *NexusConn) recvWorker() {
	defer nc.Close()
	dec := json.NewDecoder(nc.connRx)
	for dec.More() {
		res := &JsonRpcRes{}
		err := dec.Decode(res)
		if err != nil {
			break
		}
		nc.resTableLock.Lock()
		ch := nc.resTable[res.Id]
		nc.resTableLock.Unlock()
		if ch != nil {
			select {
			case ch <- res:
			default:
			}
		}
	}
}

func (nc *NexusConn) newId() (uint64, chan *JsonRpcRes) {
	id := atomic.AddUint64(&nc.reqCount, 1)
	ch := make(chan *JsonRpcRes, 1)
	nc.resTableLock.Lock()
	defer nc.resTableLock.Unlock()
	nc.resTable[id] = ch
	return id, ch
}

func (nc *NexusConn) delId(id uint64) {
	nc.resTableLock.Lock()
	delete(nc.resTable, id)
	nc.resTableLock.Unlock()
}

// GetContext returns internal connection context.
func (nc *NexusConn) GetContext() context.Context {
	return nc.context
}

// Close closes nexus connection.
func (nc *NexusConn) Close() {
	if atomic.CompareAndSwapInt32(&nc.closed, 0, 1) {
		nc.cancelFun()
		nc.conn.Close()
	}
}

// Closed returns Nexus connection state.
func (nc *NexusConn) Closed() bool {
	return atomic.LoadInt32(&nc.closed) == 1
}

// ExecNoWait is a low level JSON-RPC call function, it don't wait response from server.
func (nc *NexusConn) ExecNoWait(method string, params interface{}) (id uint64, rch chan *JsonRpcRes, err error) {
	if nc.Closed() {
		err = NewJsonRpcErr(ErrConnClosed, "", nil)
		return
	}

	if method != "sys.ping" && nc.inactivityTimeout > 0 {
		nc.inactivityTimer.Reset(nc.inactivityTimeout)
	}

	id, rch = nc.newId()
	req := &JsonRpcReq{
		Id:     id,
		Method: method,
		Params: params,
	}
	err = nc.pushReq(req)
	if err != nil {
		nc.delId(id)
		return 0, nil, err
	}
	return
}

// ExecCtx is a low level JSON-RPC call function with context support.
// It records OTel metrics and creates a client span for every call. If ctx carries an active span it
// becomes the parent; otherwise a root span is created. The W3C traceparent of the active span is
// injected into params["@metadata"]["traceparent"] so the server side can correlate traces end-to-end.
func (nc *NexusConn) ExecCtx(ctx context.Context, method string, params interface{}) (result interface{}, err error) {
	return nc.execCtx(ctx, method, params)
}

// execCtx is ExecCtx with support for extra OTel attributes on the call span
// and its metrics.
func (nc *NexusConn) execCtx(ctx context.Context, method string, params interface{}, extra ...attribute.KeyValue) (result interface{}, err error) {
	start := time.Now()
	ctx, span := startClientSpan(ctx, method, extra...)
	defer func() { endClientSpan(span, err) }()

	id, rch, err := nc.ExecNoWait(method, injectTraceparent(ctx, params))
	if err != nil {
		recordRPCCall(ctx, start, method, err, extra...)
		return nil, err
	}
	defer nc.delId(id)

	select {
	case res := <-rch:
		if res.Error != nil {
			err = res.Error
		} else {
			result = res.Result
		}
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrConnClosed, "", nil)
	}

	recordRPCCall(ctx, start, method, err, extra...)
	return
}

// Exec is a low level JSON-RPC call function.
// It delegates to ExecCtx with context.Background(), producing a root span.
// Prefer ExecCtx when a propagated context is available.
func (nc *NexusConn) Exec(method string, params interface{}) (result interface{}, err error) {
	return nc.ExecCtx(context.Background(), method, params)
}

// PingCtx pings Nexus server, timeout is the max time waiting for server response,
// after that ErrTimeout is returned. Records OTel metrics and creates a client span.
func (nc *NexusConn) PingCtx(ctx context.Context, timeout time.Duration) (err error) {
	start := time.Now()
	ctx, span := startClientSpan(ctx, "sys.ping")
	defer func() { endClientSpan(span, err); recordRPCCall(ctx, start, "sys.ping", err) }()

	id, rch, err := nc.ExecNoWait("sys.ping", injectTraceparent(ctx, nil))
	if err != nil {
		return
	}
	defer nc.delId(id)
	select {
	case <-rch:
		break
	case <-time.After(timeout):
		err = NewJsonRpcErr(ErrTimeout, "", nil)
	case <-nc.context.Done():
		err = NewJsonRpcErr(ErrConnClosed, "", nil)
	}
	return
}

// Ping pings Nexus server, timeout is the max time waiting for server response,
// after that ErrTimeout is returned.
func (nc *NexusConn) Ping(timeout time.Duration) (err error) {
	return nc.PingCtx(context.Background(), timeout)
}

// SetSessionExpirationTimeout sets an expiration timeout. When it is reached the connection will be closed
func (nc *NexusConn) SetInactivityTimeout(timeout time.Duration) (err error) {
	if nc.Closed() {
		err = NewJsonRpcErr(ErrConnClosed, "", nil)
		return
	}
	nc.inactivityTimeout = timeout
	if timeout == 0 {
		if !nc.inactivityTimer.Stop() {
			<-nc.inactivityTimer.C
		}
	} else {
		nc.inactivityTimer.Reset(timeout)
	}
	return
}

// Login attempts to login using user and pass.
// Returns the response object from Nexus or error.
func (nc *NexusConn) Login(user string, pass string) (interface{}, error) {
	par := map[string]interface{}{
		"user": user,
		"pass": pass,
	}
	res, err := nc.Exec("sys.login", par)
	if err != nil {
		return nil, err
	}
	nc.connId = ei.N(res).M("connid").StringZ()
	return res, nil
}

// Version retrieves the server version
// Returns the response object from Nexus or error.
func (nc *NexusConn) Version() (interface{}, error) {
	par := map[string]interface{}{}
	res, err := nc.Exec("sys.version", par)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Id returns the connection id after a login.
func (nc *NexusConn) Id() string {
	return nc.connId
}
