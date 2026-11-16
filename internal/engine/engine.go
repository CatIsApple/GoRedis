package engine

import (
	"context"

	"github.com/CatIsApple/GoRedis/internal/resp"
)

// 각 연결은 같은 requests 채널에 명령을 보내고, 요청에 담긴 reply 채널로 결과를 받는다.
type request struct {
	command resp.Value
	reply   chan resp.Value
}

// Engine은 한 서버의 모든 연결에서 제출한 명령을 하나의 고루틴에서 실행한다.
type Engine struct {
	requests chan request
	done     chan struct{}
}

func New(ctx context.Context) *Engine {
	e := &Engine{requests: make(chan request), done: make(chan struct{})}
	go e.run(ctx)
	return e
}

func (e *Engine) run(ctx context.Context) {
	defer close(e.done)
	for {
		select {
		case <-ctx.Done():
			return
		case req := <-e.requests:
			req.reply <- execute(req.command)
		}
	}
}

// Execute는 명령의 응답을 기다린다. 실행기가 종료됐으면 ok는 false다.
func (e *Engine) Execute(command resp.Value) (reply resp.Value, ok bool) {
	req := request{command: command, reply: make(chan resp.Value, 1)}
	select {
	case e.requests <- req:
		return <-req.reply, true
	case <-e.done:
		return resp.Value{}, false
	}
}

// Wait는 실행기 고루틴이 종료될 때까지 기다린다.
func (e *Engine) Wait() {
	<-e.done
}
