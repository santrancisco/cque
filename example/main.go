package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hashicorp/logutils"
	cque "github.com/santrancisco/cque"
)

func main() {
	// Configuring our log level
	filter := &logutils.LevelFilter{
		Levels:   []logutils.LogLevel{"DEBUG", "ERROR", "INFO"},
		MinLevel: logutils.LogLevel("ERROR"),
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)
	// ------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := cque.NewQue()
	wpool := cque.NewWorkerPool(c, cque.WorkMap{
		"foo": FooJob,
		"bar": BarJob,
	}, 5)

	wpool.Start(ctx)

	for i := 1; i <= 100; i++ {
		c.Enqueue(cque.Job{Type: "foo", Args: Foo{a: "foo", b: i}})
		c.Enqueue(cque.Job{Type: "bar", Args: fmt.Sprintf("tah! %d ", i)})
	}
	time.Sleep(2 * time.Second)
	cancel()
}

type Foo struct {
	a string
	b int
}

func FooJob(j *cque.Job) error {
	foo := j.Args.(Foo)
	log.Printf("[FOO] a = %s; b = %d \n", foo.a, foo.b)
	return nil
}

func BarJob(j *cque.Job) error {
	bar := j.Args.(string)
	log.Printf("[BAR] %s \n", bar)
	return nil
}
