package cque

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

// generic WorkFunc is a function that performs a Job. If an error is returned, the job
// is reenqueued with exponential backoff.
type WorkFunc func(j *Job) error

// WorkMap is a map of Job names to WorkFuncs that are used to perform Jobs of a
// given type.
type WorkMap map[string]WorkFunc

// Worker is a single worker that pulls jobs off the specified Queue. If no Job
// is found, the Worker will sleep for Interval seconds.
type Worker struct {
	// Interval is the amount of time that this Worker should sleep before trying
	// to find another Job.
	id       int
	ctx      context.Context
	Interval time.Duration
	c        *Client
	m        WorkMap
	mu       sync.Mutex
	done     bool
}

func NewWorker(ctx context.Context, id int, c *Client, m WorkMap) *Worker {
	return &Worker{
		id:       id,
		ctx:      ctx,
		Interval: defaultWakeInterval,
		c:        c,
		m:        m,
	}
}

// Work pulls jobs off the Worker's Queue at its Interval. This function only
// returns after Shutdown() is called, so it should be run in its own goroutine.
func (w *Worker) Work() {
	for {
		// Using select for non-blocking reading from channels.
		select {
		case <-w.ctx.Done(): // We don't want to wait until interval reaches to check if main thread has been cancelled and our job just finished
			log.Printf("[DEBUG] Worker %d done\n", w.id)
			return
		case <-time.After(w.Interval):
			w.c.workerstatus <- WorkerWaitStatus{w.id, true}
			log.Printf("[DEBUG] Worker %d wait for new job\n", w.id)
			select {
			case <-w.ctx.Done():
				log.Printf("[DEBUG] Worker %d done\n", w.id)
				return
			case j := <-w.c.pool:
				w.c.workerstatus <- WorkerWaitStatus{w.id, false}
				w.WorkOne(&j)
			}
		}
	}
}

// Manager is a single "worker" that check for when the job channel is empty
// This is done by checking the workerstatus channel and look for instances where all workers are
// waiting for job. This also only works because channel in GO are FIFO so we can get latest workers statuses
type Manager struct {
	// Interval is the amount of time that this Worker should sleep before trying
	// to find another Job.
	workercount       int
	workerswaitstatus []bool
	ctx               context.Context
	Interval          time.Duration
	c                 *Client
	mu                sync.Mutex
	done              bool
}

func NewManager(ctx context.Context, workercount int, c *Client) *Manager {
	return &Manager{
		workercount:       workercount,
		ctx:               ctx,
		workerswaitstatus: make([]bool, workercount),
		Interval:          defaultWakeInterval,
		c:                 c,
	}
}

// Start starts all of the Workers in the WorkerPool.
func (m *Manager) Manage() {
	for {
		// Using select for non-blocking reading from channels.
		select {
		case <-m.ctx.Done(): // We don't want to wait until interval reaches to check if main thread has been cancelled and our job just finished
			log.Printf("[DEBUG] Manager is done\n")
			return
		case <-time.After(m.Interval):
			select {
			case <-m.ctx.Done():
				log.Printf("[DEBUG] Manager is done\n")
				return
			case ws := <-m.c.workerstatus:
				m.workerswaitstatus[ws.Id] = ws.WaitStatus
				allworkerarewaiting := true
				for _, waitstatus := range m.workerswaitstatus {
					if !waitstatus {
						// if any worker is still working, move on
						allworkerarewaiting = false
						break
					}
				}
				m.mu.Lock()
				if allworkerarewaiting {
					log.Printf("[DEBUG] All workers are waiting for new job.")
					m.c.IsQueueEmpty = true
				} else {
					m.c.IsQueueEmpty = false
				}
				m.mu.Unlock()

			}
		}
	}
}

type ErrorMsg struct {
	msg string
	j   Job
}

func (w *Worker) WorkOne(j *Job) {

	log.Printf("[DEBUG] Start a %s job\n", j.Type)
	wf, ok := w.m[j.Type]
	if !ok {
		msg := fmt.Sprintf("Unknown job type: %q", j.Type)
		j.Error(msg)
		return
	}
	if err := wf(j); err != nil {
		j.Error(err.Error())
		return
	}
	return
}

// By default we have no wait to fetch a new job.
var defaultWakeInterval = time.Duration(0)

func init() {
	if v := os.Getenv("QUE_WAKE_INTERVAL"); v != "" {
		if newInt, err := strconv.Atoi(v); err == nil {
			defaultWakeInterval = time.Duration(newInt) * time.Second
		}
	}
}

// NewWorker returns a Worker that fetches Jobs from the JobChannel and executes
// them using WorkMap. If the type of Job is not registered in the WorkMap, it's
// considered an error and the job is re-enqueued with a backoff.
// func NewWorker(c *Client, m WorkMap) *Worker {
// 	return &Worker{
// 		Interval: defaultWakeInterval,
// 		j:        j,
// 		m:        m,
// 		ch:       make(chan struct{}),
// 		err:      make(chan struct{}),
// 	}
// }

// WorkerPool is a pool of Workers, each working jobs from the queue Queue
// at the specified Interval using the WorkMap.
type WorkerPool struct {
	ctx      context.Context
	WorkMap  WorkMap
	Interval time.Duration
	c        *Client
	manager  *Manager
	workers  []*Worker
	mu       sync.Mutex
	done     bool
}

// NewWorkerPool creates a new WorkerPool with count workers using the Client c.
func NewWorkerPool(c *Client, wm WorkMap, count int) *WorkerPool {
	return &WorkerPool{
		c:        c,
		WorkMap:  wm,
		Interval: defaultWakeInterval,
		manager:  nil,
		workers:  make([]*Worker, count),
	}
}

// Start starts all of the Workers in the WorkerPool.
func (w *WorkerPool) Start(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, _ := range w.workers {
		w.workers[i] = NewWorker(ctx, i, w.c, w.WorkMap)
		w.workers[i].Interval = w.Interval
		go w.workers[i].Work()
	}
	w.manager = NewManager(ctx, len(w.workers), w.c)
	go w.manager.Manage()
}
