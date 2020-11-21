package mapreduce

import (
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
	return mapReduce(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, cancel func(error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, SetWorkers(len(fns)))
}

func ListFinish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}
	return listMapReduce(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, cancel func(error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	})
}

func SetWorkers(workers int) OptionFn {
	return func(opts *Options) {
		opts.workers = workers
	}
}

func mapReduce(sendfnc SendFunc, mapper MapperFunc, opts ...OptionFn) error {
	options := buildOptions(opts...)
	sources := buildSource(sendfnc)
	return executeMappers(mapper,sources, options.workers)
}

func listMapReduce(sendfnc SendFunc, mapper MapperFunc) error {
	sources := buildSource(sendfnc)
	return executeListMappers(mapper,sources)
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

func executeMappers(mapper MapperFunc, sends <-chan interface{}, workers int) error {
	var wg sync.WaitGroup
	wg.Add(workers)

	pool := make(chan interface{}, workers)
	stopCh := make(chan bool)
	goSafe(func() {
		for send := range sends {
			pool <- send
		}
		wg.Wait()
		stopCh <- true
	})

	var errObj error

	cancel := once(func(err error) {
		if err != nil {
			stopCh <- true
			errObj = err
		}
	})
	for {
		select {
		case <-stopCh :
			return errObj
		case item := <- pool:
			go func() {
				defer func() {
					wg.Done()
				}()
				mapper(item, cancel)
			}()
		}
	}
}

func executeListMappers(mapper MapperFunc, sends <-chan interface{}) error {
	pool := make(chan interface{})
	defer func() {
		close(pool)
	}()

	stopCh := make(chan bool)
	goSafe(func() {
		for send := range sends {
			pool <- send
		}
		stopCh <- true
	})

	var errObj error

	cancel := once(func(err error) {
		if err != nil {
			stopCh <- true
			errObj = err
		}
	})

	for {
		select {
		case <-stopCh :
			return errObj
		case item := <- pool:
			mapper(item, cancel)
		}
	}
}


//安全使用goroutine
func goSafe(fn func()) {
	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Println(e)
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
