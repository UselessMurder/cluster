package models

import pb "../connecter"

type ReqMsg struct {
	Msg *pb.Msg
	Err error
}
