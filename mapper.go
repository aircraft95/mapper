package mapper

import (
	"log"
	"sync"
)

type (
	OptionFn func(opts *Options)
	Options  struct {
		workers int
	}
	CreateFunc func(source chan<- interface{})
	MapperFunc func(item interface{}, cancel func(error))
)

const defaultWorkers = 20

func MapperFinish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}

	return mapper(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, cancel func(error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, setWorkers(len(fns)))
}

func ListFinish(fns ...func() error) error {
	if len(fns) == 0 {
		return nil
	}
	//跟MapperFinish比较,只需要加一个锁,就能按顺序执行,这就是抽象出CreateFunc跟MapperFunc的原因
	var finishLock sync.Mutex
	return mapper(func(source chan<- interface{}) {
		for _, fn := range fns {
			finishLock.Lock()
			source <- fn
		}
	}, func(item interface{}, cancel func(error)) {
		defer finishLock.Unlock()
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, setWorkers(len(fns)))
}

func setWorkers(workers int) OptionFn {
	return func(opts *Options) {
		opts.workers = workers
	}
}

func mapper(createFnc CreateFunc, mapperFunc MapperFunc, opts ...OptionFn) error {
	options := buildOptions(opts...)
	sources := buildSource(createFnc)
	return handleMappers(mapperFunc, sources, options.workers)
}

func handleMappers(mapperFunc MapperFunc, sources <-chan interface{}, workers int) error {
	var wg sync.WaitGroup
	wg.Add(workers)

	stopCh := make(chan bool)
	goSafe(func() {
		wg.Wait()
		stopCh <- true
	})

	var errObj error

	once := new(sync.Once)
	cancel := func(err error) {
		once.Do(func() {
			if err != nil {
				errObj = err
				stopCh <- true
			}
		})
	}
	for {
		select {
		case <-stopCh:
			return errObj
		case item, ok := <-sources:
			if ok {
				go func() {
					defer wg.Done()
					mapperFunc(item, cancel)
				}()
			}
		}
	}
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

func buildSource(createFunc CreateFunc) chan interface{} {
	source := make(chan interface{})
	goSafe(func() {
		defer close(source)
		createFunc(source)
	})

	return source
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
