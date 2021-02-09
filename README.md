# mapper

install：
```
go get -u github.com/aircraft95/mapper
```

use：

MapperFinish()

```go
package main

import (
	"fmt"
	"github.com/aircraft95/mapper"
)


func main() {
	//并发执行
	err  := mapper.MapperFinish(func() error {
		fmt.Println(1)
		return nil
	}, func() error {
		fmt.Println(2)
		return errors.New("return MapperFinish err")
	}, func() error {
		fmt.Println(3)
		return nil
	})

	if err != nil {
		fmt.Printf("the error is %s", err.Error())
	}
}
```

example print: 
```
2
the error is return MapperFinish err
```

ListFinish()

```go
package main

import (
	"fmt"
	"github.com/aircraft95/mapper"
)


func main() {
	//串行执行
	err := mapper.ListFinish(func() error {
		fmt.Println(1)
		return nil
	}, func() error {
		fmt.Println(2)
		return errors.New("return ListFinish err")
	}, func() error {
		fmt.Println(3)
		return nil
	})

	if err != nil {
		fmt.Printf("the error is %s", err.Error())
	}
}
```

always print: 
```
1
2
the error is return ListFinish err
```
Tasks are executed in order 




