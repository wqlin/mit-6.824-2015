package mapreduce

import "container/list"
import "fmt"

type IntSet map[int]struct{}

type WorkerInfo struct {
	address string
}

type jobInfo struct {
	worker    string
	jobNumber int
	status    bool
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) schedule(operation JobType, nJobs int, numOtherPhase int) {
	f := func(worker string, file string, operation JobType, jobNumber int, numOtherPhase int, infoCh chan<- jobInfo) {
		args := DoJobArgs{
			File:          file,
			Operation:     operation,
			JobNumber:     jobNumber,
			NumOtherPhase: numOtherPhase}
		reply := DoJobReply{}
		info := jobInfo{worker: worker, jobNumber: jobNumber, status: true}
		if !call(worker, "Worker.DoJob", &args, &reply) {
			info.status = false
		}
		infoCh <- info
	}

	numSuccessJob := 0
	infoCh := make(chan jobInfo)
	jobs := make(IntSet)
	for i := 0; i < nJobs; i++ {
		jobs[i] = struct{}{}
	}
	for numSuccessJob < nJobs {
		select {
		case info := <-infoCh:
			if info.status {
				numSuccessJob += 1
				mr.Workers[info.worker] = &WorkerInfo{info.worker}
			} else {
				if len(mr.Workers) != 0 {
					for w, workerInfo := range mr.Workers {
						go f(workerInfo.address, mr.file, operation, info.jobNumber, numOtherPhase, infoCh)
						delete(mr.Workers, w)
						break
					}
				} else { // temporary no worker availble
					jobs[info.jobNumber] = struct{}{}
				}
			}
		case worker := <-mr.registerChannel:
			if len(jobs) != 0 {
				for job, _ := range jobs {
					go f(worker, mr.file, operation, job, numOtherPhase, infoCh)
					delete(jobs, job)
					break
				}
			} else {
				mr.Workers[worker] = &WorkerInfo{worker}
			}
		default:
			for job, _ := range jobs {
				if len(mr.Workers) != 0 {
					for w, workerInfo := range mr.Workers {
						go f(workerInfo.address, mr.file, operation, job, numOtherPhase, infoCh)
						delete(mr.Workers, w)
						delete(jobs, job)
						break
					}
				} else {
					break
				}
			}
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
	mr.schedule(Map, mr.nMap, mr.nReduce)
	mr.schedule(Reduce, mr.nReduce, mr.nMap)
	return mr.KillWorkers()
}
