package raft

import (
	"sync/atomic"
	"time"
	"util"
)

type commitCallbackInfo struct {
	callback func(result interface{}, ok bool)
	term     int
}

// ApplyFunc is the apply function pointer
type ApplyFunc func(state interface{}, op interface{}) (newState interface{}, result interface{})

// SnapshotFunc is the snapshot function pointer
type SnapshotFunc func(state interface{}) (snapshot interface{})

// Helper is the wrapper around raft algorithm
type Helper struct {
	mu             util.Mutex
	me             int
	rf             *Raft
	applyCh        chan ApplyMsg
	killCh         chan struct{}
	killed         bool
	maxLogSize     int
	commitCallback map[int][]commitCallbackInfo // commit index -> corresponding callback
	term           int
	index          int

	state interface{}

	applyFn    ApplyFunc
	snapshotFn SnapshotFunc
}

// NOTE: callback is called AFTER op applied, with mutex held
func (rh *Helper) registerCommitCallback(index, term int, callback func(result interface{}, ok bool)) {
	rh.mu.AssertHeld()
	rh.commitCallback[index] = append(rh.commitCallback[index],
		commitCallbackInfo{
			term:     term,
			callback: callback,
		})
}

func (rh *Helper) checkShouldSnapshot() {
	rh.mu.AssertHeld()
	if rh.maxLogSize == -1 {
		return
	}
	sz := rh.rf.GetStateSize()
	if sz >= rh.maxLogSize {
		// generate snapshot, call raft
		snap := rh.snapshotFn(rh.state)
		rh.rf.UpdateSnapshot(rh.term, rh.index, snap)
	}
}

// Execute an operation using raft
// Function returns only when operation commited or failed
func (rh *Helper) Execute(op interface{}) (isleader bool, committed bool, result interface{}) {
	rh.mu.Lock()

	if rh.killed {
		rh.mu.Unlock()
		return false, false, nil
	}

	index, term, isleader := rh.rf.Start(op)
	if !isleader {
		rh.mu.Unlock()
		return false, false, nil
	}

	DPrintf("Raft.Helper #%d: leader executing op %v", rh.me, op)

	// flag is used to eliminate race condition
	// between callback & timeout. whoever captured
	// the flag is allowed to progress
	flag := int32(0)

	// channel used to notify completion
	ch := make(chan interface{}, 1)

	rh.registerCommitCallback(index, term, func(result interface{}, success bool) {
		rh.mu.AssertHeld()
		if success && atomic.CompareAndSwapInt32(&flag, 0, 1) {
			// commit succeeded and flag captured, execute callback
			ch <- result
		} else {
			// commit failed or flag not captured
			// close channel to notify failure
			close(ch)
		}
	})

	rh.mu.Unlock()

	select {
	case result, ok := <-ch:
		return true, ok, result
	case <-time.After(200 * time.Millisecond):
		if atomic.CompareAndSwapInt32(&flag, 0, 1) {
			// flag captured, actual timeout
			return false, false, nil
		}
		// flag not captured, callback succeeded
		result, ok := <-ch
		return true, ok, result
	}
}

func (rh *Helper) startApplyWorker() {
	go func() {
		for {
			select {
			case <-rh.killCh:
				rh.mu.Lock()
				for _, cbs := range rh.commitCallback {
					for _, cb := range cbs {
						cb.callback(nil, false)
					}
				}
				rh.mu.Unlock()
				return
			case msg := <-rh.applyCh:
				if msg.CommandValid {
					rh.mu.Lock()
					op := msg.Command
					DPrintf("Raft.Helper #%d: applying command, term=%d, index=%d, cmdTerm=%d, cmdIndex=%d",
						rh.me, rh.term, rh.index, msg.CommandTerm, msg.CommandIndex)
					rh.term = msg.CommandTerm
					rh.index = msg.CommandIndex
					// apply the message
					newState, result := rh.applyFn(rh.state, op)
					rh.state = newState
					// execute commit callback if exist
					if cbs, ok := rh.commitCallback[msg.CommandIndex]; ok {
						delete(rh.commitCallback, msg.CommandIndex)
						for _, cb := range cbs {
							cb.callback(result, cb.term == msg.CommandTerm)
						}
					}
					rh.checkShouldSnapshot()
					rh.mu.Unlock()
				} else if msg.SnapshotValid {
					rh.mu.Lock()
					DPrintf("Raft.Helper #%d: applying snapshot, term=%d, index=%d, snapTerm=%d, snapIndex=%d",
						rh.me, rh.term, rh.index, msg.SnapshotLastTerm, msg.SnapshotLastIndex)
					util.Assert(msg.SnapshotLastTerm >= rh.term &&
						msg.SnapshotLastIndex >= rh.index, "term & index should be monotonic")
					prevIndex := rh.index
					rh.term, rh.index = msg.SnapshotLastTerm, msg.SnapshotLastIndex
					// remove previous callback (avoid memory leak)
					for i := prevIndex; i <= rh.index; i++ {
						delete(rh.commitCallback, i)
					}
					rh.state = msg.Snapshot
					rh.mu.Unlock()
				} else {
					// TODO: command not valid?
					panic("not supported yet")
				}
			}
		}
	}()
}

// Kill the Helper & underlying Raft
func (rh *Helper) Kill() {
	rh.killCh <- struct{}{}

	rh.mu.Lock()
	rh.killed = true
	rh.mu.Unlock()

	rh.rf.Kill()
}

// MakeHelper makes a raft helper
func MakeHelper(
	rf *Raft, rfApplyCh chan ApplyMsg, me int,
	maxLogSize int, defaultState interface{},
	applyFn ApplyFunc, snapshotFn SnapshotFunc) *Helper {

	rh := &Helper{
		me:             me,
		rf:             rf,
		applyFn:        applyFn,
		snapshotFn:     snapshotFn,
		maxLogSize:     maxLogSize,
		state:          defaultState,
		commitCallback: make(map[int][]commitCallbackInfo),
		killCh:         make(chan struct{}),
		applyCh:        rfApplyCh,
	}
	rh.startApplyWorker()
	return rh
}
