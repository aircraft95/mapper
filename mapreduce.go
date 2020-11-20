package mapreduce

import (
	"context"
	"log"
	"sync"
)

type (
	OptionFn          func(opts *Options)
	Options struct {
		workers int
	}
	SendFunc    func(source chan<- interface{})
	MapperFunc      func(item interface{}, cancel func(error))
)

const defaultWorkers = 20

func MapperFinish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}
	ctx,_ := context.WithCancel(context.Background())
	return mapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, cancel func(error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, ctx, SetWorkers(len(fns)))
}

func SetWorkers(workers int) OptionFn {
	return func(opts *Options) {
			opts.workers = workers
	}
}

func mapReduceVoid(sendfnc SendFunc, mapper MapperFunc, ctx context.Context, opts ...OptionFn) error {
	options := buildOptions(opts...)
	sources := buildSource(sendfnc)
	return executeMappers(mapper,sources,ctx, options.workers)
}


func buildOptions(opts ...OptionFn) *Options {
	options := newOptions()
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func newOptions() *Options {
	return &Options{
		workers: defaultWorkers,
	}
}

func buildSource(sendFunc SendFunc) chan interface{} {
	source := make(chan interface{})
	goSafe(func() {
		defer close(source)
		sendFunc(source)
	})

	return source
}

func executeMappers(mapper MapperFunc, sends <-chan interface{}, ctx context.Context, workers int) error {
	var wg sync.WaitGroup
	pool := make(chan interface{}, workers)

	defer func() {
		wg.Wait()
		close(pool)
	}()

	go func() {
		for send := range sends {
			pool <- send
		}
	}()

	cancel := once(func(err error) {
		if err != nil {
			log.Fatal(err)
		}
	})

	for {
		select {
		case <- ctx.Done():
			drain(sends)
			return nil
		case item := <- pool:
			wg.Add(1)
			goSafe(func() {
				defer func() {
					wg.Done()
					<- pool
				}()
				mapper(item, cancel)
			})
		}
	}
}


//安全使用goroutine
func goSafe(fn func()) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Fatal(e)
			}
		}()
		fn()
	}()
}

func once(fn func(error)) func(error) {
	once := new(sync.Once)
	return func(err error) {
		once.Do(func() {
			fn(err)
		})
	}
}

// 排空channel的数据， 用于发生错误 强制退出时清空信道的数据
func drain(channel <-chan interface{}) {
	for range channel {
	}
}

