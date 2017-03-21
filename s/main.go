package main

import (
	"../models"
	"../primary"
	"./router"
	"fmt"
	"math"
	"math/rand"
	"runtime/debug"
	"strconv"
	"time"
)

type userShell struct {
	r *router.Router
}

func Create(r *router.Router) *userShell {
	us := &userShell{}
	us.r = r
	return us
}

func (us *userShell) Serve() {
	for {
		fmt.Println("Enter command:")
		var str string
		fmt.Scanln(&str)
		switch str {
		case "exit":
			us.Exit()
		case "clear":
			us.Clear()
		case "scan":
			us.Scan()
		case "show":
			us.Show()
		case "create":
			us.Create()
		case "remove":
			us.Remove()
		default:
			fmt.Println("Invalid command!")
		}
	}
}

func (us *userShell) Exit() {
	fmt.Println("Turn off clients?(y/n)")
	if primary.YesNo() {
		us.r.Exit(1)
	} else {
		us.r.Exit(0)
	}
}

func (us *userShell) Clear() {
	fmt.Println("Clear clients?(y/n)")
	if primary.YesNo() {
		us.r.Clear(1)
	} else {
		us.r.Clear(0)
	}
}

func (us *userShell) Scan() {
	us.r.Scan()
	clients := us.r.GetClients()
	switch len(*clients) {
	case 0:
		fmt.Println("No clients found!")
	case 1:
		fmt.Println("Find 1 client:")
		for key, value := range *clients {
			fmt.Println(key, ":", value)
		}
	default:
		str := strconv.Itoa(len(*clients))
		fmt.Println("Find", str, "clients:")
		for key, value := range *clients {
			fmt.Println(key, ":", value)
		}
	}
}

func (us *userShell) Show() {
	fmt.Println("What to show?")
	var str string
	fmt.Scanln(&str)
	switch str {
	case "groups":
		us.ShowGroups()
	case "result":
		us.ShowResult()
	default:
		fmt.Println("Invalid type!")
	}
}

func (us *userShell) ShowGroups() {
	groups := us.r.GetGroups()
	fmt.Println("Active:")
	for _, value := range (*groups)[0] {
		fmt.Println(value)
	}
	fmt.Println("Finished:")
	for _, value := range (*groups)[1] {
		fmt.Println(value)
	}
	fmt.Println("Failds:")
	for _, value := range (*groups)[2] {
		fmt.Println(value)
	}
}

func (us *userShell) ShowResult() {
	fmt.Println("Get id:")
	var str string
	fmt.Scanln(&str)
	id, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	var result interface{}
	result, err = us.r.ShowGroup(id)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	switch result.(type) {
	case *models.PiBase:
		us.ShowPi(result.(*models.PiBase))
	case *models.VMatrixBase:
		us.ShowVMatrix(result.(*models.VMatrixBase))
	case *models.MMatrixBase:
		us.ShowMMatrix(result.(*models.MMatrixBase))
	case *models.SortBase:
		us.ShowSort(result.(*models.SortBase))
	}
}

func (us *userShell) ShowPi(data *models.PiBase) {
	fmt.Println("Radius:", data.Radius)
	fmt.Println("Iterations:", data.Iterations)
	fmt.Println("Pi:", data.Result)
	fmt.Println("Processors:", data.WorkerCount)
	fmt.Println("Time:", data.End.Sub(data.Begin))
}

func (us *userShell) ShowVMatrix(data *models.VMatrixBase) {
	fmt.Println("Show Matrix?")
	if primary.YesNo() {
		fmt.Println("Matrix:")
		fmt.Println(data.Matrix)
	}
	fmt.Println("Show Vector?")
	if primary.YesNo() {
		fmt.Println("Vector:")
		fmt.Println(data.Vector)
	}
	fmt.Println("Show Result?")
	if primary.YesNo() {
		fmt.Println("Result:")
		fmt.Println(data.ResultVector)
	}
	fmt.Println("Processors:", data.WorkerCount)
	fmt.Println("Size:", data.Size)
	fmt.Println("Time:", data.End.Sub(data.Begin))
}

func (us *userShell) ShowMMatrix(data *models.MMatrixBase) {
	fmt.Println("Show MatrixA?")
	if primary.YesNo() {
		fmt.Println("MatrixA:")
		fmt.Println(data.AMatrix)
	}
	fmt.Println("Show MatrixB?")
	if primary.YesNo() {
		fmt.Println("MatrixB:")
		fmt.Println(data.BMatrix)
	}
	fmt.Println("Show test?")
	if primary.YesNo() {
		fmt.Println("test:")
		for i := uint64(0); i < data.Size; i++ {
			for j := uint64(0); j < data.Size; j++ {
				fmt.Print(MulMatrix(data.AMatrix, data.BMatrix, data.Size, i, j), " ")
			}
			fmt.Print("\n")
		}
	}
	fmt.Println("Show MatrixC?")
	if primary.YesNo() {
		fmt.Println("MatrixC:")
		fmt.Println(data.CMatrix)
	}
	fmt.Println("Processors:", data.WorkerCount)
	fmt.Println("Size:", data.Size)
	fmt.Println("Time:", data.End.Sub(data.Begin))
}

func MulMatrix(A [][]float64, B [][]float64, size, ti, tj uint64) float64 {
	result := float64(0)
	for j, i := uint64(0), uint64(0); j < size && i < size; j++ {
		result += A[ti][j] * B[i][tj]
		i++
	}
	return result
}

func (us *userShell) ShowSort(data *models.SortBase) {
	fmt.Println("Show original data?")
	if primary.YesNo() {
		fmt.Println("Original data:")
		fmt.Println(data.DataLine)
	}
	fmt.Println("Show sorted data?")
	if primary.YesNo() {
		fmt.Println("Sorted data:")
		fmt.Println(data.SortLine)
	}
	fmt.Println("Processors:", data.WorkerCount)
	fmt.Println("Size:", data.Size)
	fmt.Println("Time:", data.End.Sub(data.Begin))
}

func (us *userShell) Create() {
	fmt.Println("What to create?")
	var str string
	fmt.Scanln(&str)
	clients := us.r.GetClients()
	if len(*clients) < 1 {
		fmt.Println("No one clients in the cluster!")
		return
	}
	switch str {
	case "pi":
		us.CreatePi(clients)
	case "vmatrix":
		us.CreateVMatrix(clients)
	case "mmatrix":
		us.CreateMMatrix(clients)
	case "sort":
		us.CreateSort(clients)
	default:
		fmt.Println("Invalid type!")
	}
}

func (us *userShell) CreatePi(clients *map[string]string) {
	var base models.PiBase
	var str string
	fmt.Println("Input Radius:")
	fmt.Scanln(&str)
	var err error
	base.Radius, err = strconv.ParseUint(str, 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}
	if base.Radius < 1 {
		fmt.Println("Value is too small!")
		return
	}
	fmt.Println("Input iterations:")
	fmt.Scanln(&str)
	base.Iterations, err = strconv.ParseUint(str, 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}
	if base.Iterations < 1 {
		fmt.Println("Value is too small!")
		return
	}
	workStation := make(map[string]uint64)
	for key, value := range *clients {
		fmt.Println("Input process num for " + value + " in " + key + ":")
		fmt.Scanln(&str)
		process, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		workStation[key] = process
		base.WorkerCount += process
	}
	if base.WorkerCount < 1 {
		fmt.Println("Value is too small!")
		return
	}
	id, err := us.r.CreateGroup(&workStation, &base)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Group id:", id)
	return
}

func (us *userShell) CreateVMatrix(clients *map[string]string) {
	var base models.VMatrixBase
	var str string
	workStation := make(map[string]uint64)
	var err error
	for key, value := range *clients {
		fmt.Println("Input process num for " + value + " in " + key + ":")
		fmt.Scanln(&str)
		process, err := strconv.ParseUint(str, 10, 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		workStation[key] = process
		base.WorkerCount += process
	}
	if base.WorkerCount < 1 {
		fmt.Println("Value is too small!")
		return
	}
	for {
		fmt.Println("Processors count:", base.WorkerCount)
		fmt.Println("Input matrix size (size >= count):")
		fmt.Scanln(&str)
		base.Size, err = strconv.ParseUint(str, 10, 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		if base.Size < base.WorkerCount {
			fmt.Println("Value is too small!")
			continue
		}
		break
	}
	rand.Seed(time.Now().Unix())
	base.Matrix = make([][]float64, base.Size)
	for i, _ := range base.Matrix {
		base.Matrix[i] = make([]float64, base.Size)
		for j, _ := range base.Matrix[i] {
			base.Matrix[i][j] = rand.NormFloat64()
		}
	}
	base.Vector = make([]float64, base.Size)
	for i, _ := range base.Vector {
		base.Vector[i] = rand.NormFloat64()
	}
	base.ResultVector = make(map[uint64]float64)
	id, err := us.r.CreateGroup(&workStation, &base)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Group id:", id)
	return
}

func (us *userShell) CreateMMatrix(clients *map[string]string) {
	var base models.MMatrixBase
	var str string
	workStation := make(map[string]uint64)
	var err error
	for {
		fmt.Println("The number of processors should be even or one (sqrt(p) == ceil)!")
		for key, value := range *clients {
			fmt.Println("Input process num for " + value + " in " + key + ":")
			fmt.Scanln(&str)
			process, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				fmt.Println(err)
				return
			}
			workStation[key] = process
			base.WorkerCount += process
		}
		if (base.WorkerCount%2 == 0 || base.WorkerCount == 1) && math.Sqrt(float64(base.WorkerCount))*math.Sqrt(float64(base.WorkerCount)) == float64(base.WorkerCount) {
			if base.WorkerCount > 0 {
				break
			}
		}
		fmt.Println("Incorrect number of processors!")
		base.WorkerCount = 0
	}
	for {
		fmt.Println("Processors count:", base.WorkerCount)
		fmt.Println("Input matrix size ( size >= count, size % count == 0):")
		fmt.Scanln(&str)
		base.Size, err = strconv.ParseUint(str, 10, 64)
		if err != nil {
			fmt.Println(err)
			return
		}
		if base.Size < base.WorkerCount || base.Size%base.WorkerCount != 0 {
			fmt.Println("Invalid size!")
			continue
		}
		break
	}
	rand.Seed(time.Now().Unix())
	base.AMatrix = make([][]float64, base.Size)
	base.BMatrix = make([][]float64, base.Size)
	base.CMatrix = make([][]float64, base.Size)
	for i, _ := range base.AMatrix {
		base.AMatrix[i] = make([]float64, base.Size)
		base.BMatrix[i] = make([]float64, base.Size)
		base.CMatrix[i] = make([]float64, base.Size)
		for j, _ := range base.AMatrix[i] {
			base.AMatrix[i][j] = rand.NormFloat64()
			base.BMatrix[i][j] = rand.NormFloat64()
		}
	}
	id, err := us.r.CreateGroup(&workStation, &base)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Group id:", id)
	return
}

func (us *userShell) CreateSort(clients *map[string]string) {
	var base models.SortBase
	var str string
	workStation := make(map[string]uint64)
	var err error
	for {
		fmt.Println("The number of processors should be even or one!")
		for key, value := range *clients {
			fmt.Println("Input process num for " + value + " in " + key + ":")
			fmt.Scanln(&str)
			process, err := strconv.ParseUint(str, 10, 64)
			if err != nil {
				fmt.Println(err)
				return
			}
			workStation[key] = process
			base.WorkerCount += process
		}
		if base.WorkerCount%2 == 0 || base.WorkerCount == 1 {
			if base.WorkerCount > 0 {
				break
			}
		}
		fmt.Println("Incorrect number of processors!")
		base.WorkerCount = 0
	}
	for {
		fmt.Println("Processors count:", base.WorkerCount)
		fmt.Println("Input data size ( size >= count, size % count == 0 ):")
		fmt.Scanln(&str)
		base.Size, err = strconv.ParseUint(str, 10, 64)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if base.Size < base.WorkerCount || base.Size%base.WorkerCount != 0 {
			fmt.Println("Invalid size!")
			continue
		}
		break
	}
	base.BlockSize = base.Size / base.WorkerCount
	base.DataLine = make([]uint32, base.Size)
	base.SortLine = make([]uint32, base.Size)
	rand.Seed(time.Now().Unix())
	for key, _ := range base.DataLine {
		base.DataLine[key] = rand.Uint32()

	}
	id, err := us.r.CreateGroup(&workStation, &base)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Group id:", id)
}

func (us *userShell) Remove() {
	fmt.Println("Get id:")
	var str string
	fmt.Scanln(&str)
	id, err := strconv.ParseUint(str, 10, 64)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	err = us.r.RemoveGroup(id)
	if err != nil {
		fmt.Println("Error:", err)
		return
	}
	fmt.Println(id, "was removed!")
}

func main() {
	debug.SetGCPercent(50)
	r := router.Create()
	go r.Handler()
	us := Create(r)
	us.Serve()
}
