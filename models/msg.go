package models

import (
	pb "../connecter"
)

type Msg struct {
	Host string
	Msg  *pb.Msg
}
