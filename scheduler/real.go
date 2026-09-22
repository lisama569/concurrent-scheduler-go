package scheduler

// startRealLoop 启动真实调度器的后台 dispatcher。
//
// 唤醒来源：
//  1. 事件信号 wakeCh（提交/取消/任务结束/暂停恢复/关闭）；
//  2. 周期 tick 计时器（排队超时判定、年龄增长、run timeout/runaway 回收），
//     由 scan 自身在锁内持续重新武装，与事件扫描共用同一条扫描路径。
//
// wake 采用"发送方等待 dispatcher 消费"的握手方式，保证批量提交时
// 每个事件都能立即触发一轮完整扫描（而不是被容量为 1 的信号合并成一次）。
func (s *Scheduler) startRealLoop() {
	s.mu.Lock()
	s.armTickLocked()
	s.mu.Unlock()

	s.dispWG.Add(1)
	go func() {
		defer s.dispWG.Done()
		for {
			select {
			case <-s.doneCh:
				return
			case ack := <-s.wakeCh:
				s.scan()
				if ack != nil {
					close(ack)
				}
			}
			if s.isClosed() {
				return
			}
		}
	}()
}

func (s *Scheduler) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// startReal 在独立 goroutine 中运行任务体，panic 在此处被恢复并隔离，
// 不影响调度器账目与其他任务。任务 goroutine 不进入任何关闭等待组：
// 失控任务可能永不退出，调度器只保证账目终态，不保证 goroutine 被回收。
func (s *Scheduler) startReal(t *task) {
	go func() {
		s.runTaskBody(t)
	}()
}

// runTaskBody 执行用户任务并完成终态结算。
//
// 退出原因判定（同一任务只结算一次）：
//   - panic：StatusPanicked（先于 ctx 检查，确保 panic 不被误判）；
//   - ctx 已取消且标记为 run timeout：StatusTimedOut；
//   - ctx 已取消（用户取消或关闭强制取消）：StatusCanceled；
//   - Fn 返回 nil / err：StatusSucceeded / StatusFailed。
func (s *Scheduler) runTaskBody(t *task) {
	var (
		err error
		pv  any
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				pv = r
			}
		}()
		err = t.spec.Fn(t.ctx)
	}()

	if pv != nil {
		s.finishTask(t, StatusPanicked, panicErr(pv))
		return
	}
	if t.ctx.Err() != nil {
		s.mu.Lock()
		timedOut := t.timedOut
		userCanceled := t.userCanceled
		s.mu.Unlock()
		if timedOut && !userCanceled {
			s.finishTask(t, StatusTimedOut, ErrTimeout)
			return
		}
		s.finishTask(t, StatusCanceled, ErrCanceled)
		return
	}
	if err != nil {
		s.finishTask(t, StatusFailed, err)
		return
	}
	s.finishTask(t, StatusSucceeded, nil)
}
