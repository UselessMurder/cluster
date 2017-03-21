package primary

import (
	"fmt"
)

func Pause() {
	fmt.Println("Pause")
	var str string
	fmt.Scanln(&str)
}
