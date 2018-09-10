package cque

import (
	"log"
)

// Job is a single unit of work for Que to perform.
type Job struct {
	Type string
	// Args can be anything depends on the job
	Args interface{}
}

// Result is the output of a Job and can be used to send something back to mainthread.
type Result struct {
	JobType string
	// Args can be anything depends on the job
	Result interface{}
}

// Client is a Que client that can add jobs to the queue and remove jobs from
// the queue.
type Client struct {
	IsQueueEmpty bool
	pool         chan Job
	workerstatus chan WorkerWaitStatus
	Result       chan Result
	// TODO: add a way to specify default queueing options
}

type WorkerWaitStatus struct {
	Id         int
	WaitStatus bool
}

// NewClient create our new local queue
// Note: we are creating 5000 queue as we expect this to not reach this high.
// If due to delay and this limit is reached, it may cause deadlock if all channels want to write
// This was implemented crudely as PoC so fixes should be considered if used in production.
// Eg: maybe a slice/array to store the jobs being added to the client and have a channel dedicated to pushing
// the job out to workers.
func NewQue() *Client {
	return &Client{
		IsQueueEmpty: true,
		pool:         make(chan Job, 5000),
		workerstatus: make(chan WorkerWaitStatus, 100),
		Result:       make(chan Result, 5000),
	}
}

// Enqueue adds a job to the queue.
func (c *Client) Enqueue(j Job) {
	c.pool <- j
}

// We dont tolerate error, all error will be logged and discarded.
func (j *Job) Error(msg string) {
	log.Printf("[ERROR] %s\n", msg)
}
