package mapreduce

import (
	"fmt"
	"sync"
	//"time"

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

type MapFileToTaskNum struct {
	File string
	TaskNum int
}

type ReduceTaskNum struct {
	 TaskNum int
}


func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	var wg sync.WaitGroup
	availableWorkerChan := make(chan string, ntasks)
	//fileToBeHandleChan := make(chan string, len(mapFiles))
	//taskNumChan := make(chan int, 10 * ntasks)
	failedTaskChanMap := make(chan MapFileToTaskNum, len(mapFiles))
	failedTaskChanReduce := make(chan ReduceTaskNum, nReduce)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)  //map : ntask = 100, n-other =50
		n_other = nReduce
		for index, file := range mapFiles{
			failedTaskChanMap <- MapFileToTaskNum{File:file, TaskNum:index}
		}
	case reducePhase:
		ntasks = nReduce    // reduce : mtask = 50 ,n-other =100
		n_other = len(mapFiles)
		for i:= 0 ;i< nReduce; i++ {
			failedTaskChanReduce <- ReduceTaskNum{TaskNum:i}
		}
	}

	fmt.Printf("	Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	wg.Add(ntasks)
    locker := new(sync.Mutex)
    successTask  :=  0
	//go func() {
	//	for {
	//		taskNumChan <- latestTaskNumProvided
	//		latestTaskNumProvided += 1
	//	}
	//}()
	go func() {
		if phase == mapPhase {
			for {
			select {
			case register := <-registerChan:
				go func() {
					d := <- failedTaskChanMap
					result := call(register, "Worker.DoTask",
						DoTaskArgs{JobName: jobName,
							File: d.File, Phase: mapPhase, TaskNumber: d.TaskNum,
							NumOtherPhase: n_other}, nil)
					if result {
						fmt.Println("map worker success--->", d.TaskNum)
						availableWorkerChan <- register
						locker.Lock()
						successTask += 1
						if successTask <= ntasks{
							wg.Done()
							locker.Unlock()
						} else {
							select {}
						}
					} else {
						fmt.Println("map worker failed--->", d.TaskNum)
						failedTaskChanMap <- d
					}

				}()

			case available_worker := <-availableWorkerChan:
				go func() {
					d := <- failedTaskChanMap
					//locker.Lock()
					result := call(available_worker, "Worker.DoTask",
						DoTaskArgs{JobName: jobName,
							File: d.File, Phase: mapPhase, TaskNumber: d.TaskNum,
							NumOtherPhase: n_other}, nil)
					if result {
						fmt.Println("map worker success--->", d.TaskNum)
						availableWorkerChan <- available_worker
						//successTask += 1
						//if successTask == ntasks {
						locker.Lock()
						successTask += 1
						if successTask <= ntasks{
							wg.Done()
							locker.Unlock()
						} else {
							select {}
						}

					} else {
						fmt.Println("map worker failed--->", d.TaskNum)
						failedTaskChanMap <- d
					}
				}()
			default:

			}
		}
		} else {
		for {
			select {
			case register := <-registerChan:
				go func() {
					d := <-failedTaskChanReduce
					//locker.Lock()
					result := call(register, "Worker.DoTask",
						DoTaskArgs{JobName: jobName,
							Phase: reducePhase, TaskNumber: d.TaskNum,
							NumOtherPhase: n_other}, nil)
					if result {
						fmt.Println("reduce worker success--->", d.TaskNum)
						availableWorkerChan <- register
						locker.Lock()
						successTask += 1
						if successTask <= ntasks{
							wg.Done()
							locker.Unlock()
						} else {
							select {}
						}
					} else {
						fmt.Println("reduce worker failed--->", d.TaskNum)
						failedTaskChanReduce <- d
					}

				}()

			case available_worker := <-availableWorkerChan:
				go func() {
					d := <-failedTaskChanReduce
					//locker.Lock()
					result := call(available_worker, "Worker.DoTask",
						DoTaskArgs{JobName: jobName,
							Phase: reducePhase, TaskNumber: d.TaskNum,
							NumOtherPhase: n_other}, nil)
					if result {
						fmt.Println("reduce worker success--->", d.TaskNum)
						availableWorkerChan <- available_worker
						locker.Lock()
						successTask += 1
						if successTask <= ntasks{
							wg.Done()
							locker.Unlock()
						} else {
							select {}
						}
					} else {
						fmt.Println("reduce worker failed--->", d)
						failedTaskChanReduce <- d
					}

				}()
			default:

			}
		}
		}
	}()

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//

	wg.Wait()

	fmt.Printf("Schedule: %v phase done\n", phase)

}
