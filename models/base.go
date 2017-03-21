package models

import (
	"time"
)

type PiBase struct {
	Begin       time.Time
	Radius      uint64
	WorkerCount uint64
	Iterations  uint64
	Result      float64
	End         time.Time
}

type VMatrixBase struct {
	Begin        time.Time
	Size         uint64
	WorkerCount  uint64
	Matrix       [][]float64
	Vector       []float64
	ResultVector map[uint64]float64
	End          time.Time
}

type MMatrixBase struct {
	Begin       time.Time
	Size        uint64
	WorkerCount uint64
	Workers     *WorkerNetwork
	AMatrix     [][]float64
	BMatrix     [][]float64
	CMatrix     [][]float64
	End         time.Time
}

type SortBase struct {
	Begin       time.Time
	Size        uint64
	Workers     *WorkerLine
	WorkerCount uint64
	DataLine    []uint32
	SortLine    []uint32
	BlockSize   uint64
	End         time.Time
}
