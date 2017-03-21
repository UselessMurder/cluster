package models

type ControlMsg struct {
	Code uint8
	Id   uint64
	Data interface{}
}

type Wrapper struct {
	Clients *map[string]uint64
	Data    interface{}
}
