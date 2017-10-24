package mapreduce

import (
    "fmt"
    "sync"
)

type TaskDone struct{
    mux sync.Mutex
    count int
}

func (t *TaskDone) value() int {
    t.mux.Lock()
    defer t.mux.Unlock()
    return t.count
}

func (t *TaskDone) inc() {
    t.mux.Lock()
    defer t.mux.Unlock()
    t.count ++
}
//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func execTask(addr string, task_args DoTaskArgs, registerChan chan string,
        queue chan int, td *TaskDone) {
    success := call(addr, "Worker.DoTask", task_args, nil)
    if success == true {
        td.inc()
    } else {
        //put self to task queue
        queue <- task_args.TaskNumber
    }
    //Important: here need to use non-blocking sending. When all the task is finished,
    //schedule() will not receive from channel.
    select {
        case registerChan <- addr:
            return
        default:
            return
    }
}

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

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
    td := new(TaskDone)
    td.count = 0
    //init task queue
    var queue = make(chan int, ntasks)
    for i:=0; i<ntasks; i++ {
        queue <- i
    }

    for td.value() < ntasks {
        //Important: here need to use non-blocking channel receiver.
        //master read td, and goroutine write td.
        // these two threads are concurrent. If blocking channel, When td.value() == 99, 
        // it will block at <- queue. Although goroutine later updates
        // td.count to 100, the main thread can never read the new value
        select {
            case taskIndex := <-queue:
                task_args := DoTaskArgs{jobName, mapFiles[taskIndex], phase, taskIndex, n_other}
                addr := <-registerChan
                go execTask(addr, task_args, registerChan, queue, td)
            default:
                //do nothing
        }
    }
	fmt.Printf("Schedule: %v phase done\n", phase)
}
