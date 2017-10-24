package mapreduce

import (
    "fmt"
    "sync"
)

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
                wg *sync.WaitGroup) {
    defer wg.Done()
    call(addr, "Worker.DoTask", task_args, nil)
    //here need to use non-blocking sending. When all the task is finished,
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
    var wg sync.WaitGroup
    for i:=0; i<ntasks; i++ {
        addr := <-registerChan
        task_args := DoTaskArgs{jobName, mapFiles[i], phase, i, n_other}
        wg.Add(1)
       // go call(addr, "Worker.DoTask", task_args, nil)
        go execTask(addr, task_args, registerChan, &wg)
    }
    wg.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}
