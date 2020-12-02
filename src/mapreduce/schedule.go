package mapreduce

import (
	"fmt"
	"sync"
)

// type workerInfo struct {
// 	free bool
// }

type schedContext struct {
	sync.Mutex
	waitGroup      sync.WaitGroup
	freeWorkerChan chan string
	doneChan       chan struct{}
}

func schedSingleTask(ctx *schedContext, args *DoTaskArgs) {
	for {
		worker := <-ctx.freeWorkerChan
		ok := call(worker, "Worker.DoTask", args, nil)
		if ok {
			ctx.freeWorkerChan <- worker
			debug("task %v done 1", *args)
			break
		}
	}

	ctx.waitGroup.Done()
	debug("task %v done 2", *args)
}

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//
	ctx := new(schedContext)
	ctx.freeWorkerChan = make(chan string, 1024)
	ctx.doneChan = make(chan struct{})

	go func() {
		for {
			select {
			case <-ctx.doneChan:
				return
			case w := <-registerChan:
				// notify there is a free worker
				ctx.freeWorkerChan <- w
			}
		}
	}()

	switch phase {
	case mapPhase:
		ctx.waitGroup.Add(len(mapFiles))
		for i, f := range mapFiles {
			args := new(DoTaskArgs)
			args.JobName = jobName
			args.File = f
			args.Phase = phase
			args.TaskNumber = i
			args.NumOtherPhase = nReduce
			go schedSingleTask(ctx, args)
		}
	case reducePhase:
		ctx.waitGroup.Add(nReduce)
		for i := 0; i < nReduce; i++ {
			args := new(DoTaskArgs)
			args.JobName = jobName
			args.Phase = phase
			args.TaskNumber = i
			args.NumOtherPhase = len(mapFiles)
			go schedSingleTask(ctx, args)
		}
	}

	ctx.waitGroup.Wait()

	close(ctx.freeWorkerChan)

	ctx.doneChan <- struct{}{}
	close(ctx.doneChan)

	fmt.Printf("Schedule: %v done\n", phase)
}
