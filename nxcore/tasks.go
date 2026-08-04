package nxcore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jaracil/ei"
)

// TaskPushCtx pushes a task to Nexus with context propagation.
// If ctx carries an active OTel span its W3C traceparent is injected into
// params["@metadata"]["traceparent"], enabling end-to-end trace correlation
// through the Nexus broker to the worker that pulls the task.
func (nc *NexusConn) TaskPushCtx(ctx context.Context, method string, params interface{}, timeout time.Duration, opts ...*TaskOpts) (interface{}, error) {
	par := ei.M{
		"method": method,
		"params": injectTraceparent(ctx, params),
	}
	if len(opts) > 0 {
		if opts[0].Priority != 0 {
			par["prio"] = opts[0].Priority
		}
		if opts[0].Ttl != 0 {
			par["ttl"] = opts[0].Ttl
		}
		if opts[0].Detach {
			par["detach"] = true
		}
	}
	if timeout > 0 {
		par["timeout"] = float64(timeout) / float64(time.Second)
	}
	return nc.ExecCtx(ctx, "task.push", par)
}

// TaskPush pushes a task to Nexus cloud.
// method is the method path Ex. "test.fibonacci.fib"
// params is the method params object.
// timeout is the maximum time waiting for response, 0 = no timeout.
// options (see TaskOpts struct)
// Returns the task result or error.
func (nc *NexusConn) TaskPush(method string, params interface{}, timeout time.Duration, opts ...*TaskOpts) (interface{}, error) {
	return nc.TaskPushCtx(context.Background(), method, params, timeout, opts...)
}

// TaskPushCh pushes a task to Nexus cloud.
// method is the method path Ex. "test.fibonacci.fib"
// params is the method params object.
// timeout is the maximum time waiting for response, 0 = no timeout.
// options (see TaskOpts struct)
// Returns two channels (one for result of interface{} type and one for error of error type).
func (nc *NexusConn) TaskPushCh(method string, params interface{}, timeout time.Duration, opts ...*TaskOpts) (<-chan interface{}, <-chan error) {
	chres := make(chan interface{}, 1)
	cherr := make(chan error, 1)
	go func() {
		res, err := nc.TaskPush(method, params, timeout, opts...)
		if err != nil {
			cherr <- err
		} else {
			chres <- res
		}

	}()
	return chres, cherr
}

// TaskPull pulls a task from Nexus cloud.
// prefix is the method prefix we want pull Ex. "test.fibonacci"
// timeout is the maximum time waiting for a task.
// Returns a new incomming Task or error.
func (nc *NexusConn) TaskPull(prefix string, timeout time.Duration) (*Task, error) {
	par := map[string]interface{}{
		"prefix": prefix,
	}
	if timeout > 0 {
		par["timeout"] = float64(timeout) / float64(time.Second)
	}
	res, err := nc.Exec("task.pull", par)
	if err != nil {
		return nil, err
	}
	t := ei.N(res)
	task := &Task{
		nc:     nc,
		Id:     t.M("taskid").StringZ(),
		Path:   t.M("path").StringZ(),
		Method: t.M("method").StringZ(),
		Params: t.M("params").RawZ(),
		Tags:   t.M("tags").MapStrZ(),
		Prio:   t.M("prio").IntZ(),
		Detach: t.M("detach").BoolZ(),
		User:   t.M("user").StringZ(),
	}
	return task, nil
}

// TaskList returns how many push/pulls are happening on a path and its children
// Returns a TaskList or error.
func (nc *NexusConn) TaskList(prefix string, limit int, skip int, opts ...*ListOpts) ([]Task, error) {
	par := map[string]interface{}{
		"prefix": prefix,
		"limit":  limit,
		"skip":   skip,
	}
	if len(opts) > 0 {
		if opts[0].LimitByDepth {
			par["depth"] = opts[0].Depth
		}
		if opts[0].Filter != "" {
			par["filter"] = opts[0].Filter
		}
	}
	res, err := nc.Exec("task.list", par)
	if err != nil {
		return nil, err
	}

	list := make([]Task, 0)
	b, err := json.Marshal(res)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &list)
	if err != nil {
		return nil, err
	}

	return list, nil
}

// TaskCount counts task pushes and pulls from Nexus task's table.
// Returns the response object from Nexus or error.
func (nc *NexusConn) TaskCount(prefix string, opts ...*CountOpts) (interface{}, error) {
	par := map[string]interface{}{
		"prefix": prefix,
	}
	if len(opts) > 0 {
		if opts[0].Subprefixes {
			par["subprefixes"] = opts[0].Subprefixes
		}
		if opts[0].Filter != "" {
			par["filter"] = opts[0].Filter
		}
	}
	return nc.Exec("task.count", par)
}

// SendResultCtx closes Task with result, propagating the OTel trace context.
// Returns the response object from Nexus or error.
func (t *Task) SendResultCtx(ctx context.Context, res interface{}) (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.Id,
		"result": res,
	}
	return t.nc.ExecCtx(ctx, "task.result", par)
}

// SendResult closes Task with result.
// Returns the response object from Nexus or error.
func (t *Task) SendResult(res interface{}) (interface{}, error) {
	return t.SendResultCtx(context.Background(), res)
}

// SendErrorCtx closes Task with error, propagating the OTel trace context.
// code is the JSON-RPC error code.
// message is optional in case of well known error code (negative values).
// data is an optional extra info object.
// Returns the response object from Nexus or error.
func (t *Task) SendErrorCtx(ctx context.Context, code int, message string, data interface{}) (interface{}, error) {
	if code < 0 {
		if errstr, ok := ErrStr[code]; ok {
			if message != "" {
				message = fmt.Sprintf("%s:[%s]", errstr, message)
			} else {
				message = errstr
			}
		}
	}
	par := map[string]interface{}{
		"taskid":  t.Id,
		"code":    code,
		"message": message,
		"data":    data,
	}
	return t.nc.ExecCtx(ctx, "task.error", par)
}

// SendError closes Task with error.
// code is the JSON-RPC error code.
// message is optional in case of well known error code (negative values).
// data is an optional extra info object.
// Returns the response object from Nexus or error.
func (t *Task) SendError(code int, message string, data interface{}) (interface{}, error) {
	return t.SendErrorCtx(context.Background(), code, message, data)
}

// RejectCtx rejects the task, propagating the OTel trace context.
// Task is returned to Nexus tasks queue.
func (t *Task) RejectCtx(ctx context.Context) (interface{}, error) {
	par := map[string]interface{}{
		"taskid": t.Id,
	}
	return t.nc.ExecCtx(ctx, "task.reject", par)
}

// Reject rejects the task. Task is returned to Nexus tasks queue.
func (t *Task) Reject() (interface{}, error) {
	return t.RejectCtx(context.Background())
}

// AcceptCtx accepts a detached task, propagating the OTel trace context.
// Is an alias for SendResultCtx(ctx, nil).
func (t *Task) AcceptCtx(ctx context.Context) (interface{}, error) {
	return t.SendResultCtx(ctx, nil)
}

// Accept accepts a detached task. Is an alias for SendResult(nil).
func (t *Task) Accept() (interface{}, error) {
	return t.AcceptCtx(context.Background())
}

// GetConn retrieves the task underlying nexus connection.
func (t *Task) GetConn() *NexusConn {
	return t.nc
}

// NewTask creates a new task with the provided Nexus connection.
// Usually tasks are created on TaskPull, but this method is usefull for testing purposes.
func NewTask(conn *NexusConn) *Task {
	return &Task{nc: conn}
}
