package primary

import "fmt"

func YesNo() bool {
	var str string
	for {
		fmt.Scanln(&str)
		switch str {
		case "y":
			return true
		case "n":
			return false
		default:
			fmt.Println("Incorrect input!")
		}
	}
	return false
}
