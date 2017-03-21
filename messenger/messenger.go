package Messenger

import (
	pb "../connecter"
	"../models"
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/reflection"
	"log"
	"math"
	"net"
)

type Messenger struct {
	out func(value *models.Msg) (*pb.Msg, error)
}

func Create(out func(value *models.Msg) (*pb.Msg, error), port string) *Messenger {
	var m Messenger
	m.out = out
	go m.listener(port)
	return &m
}

func (m *Messenger) SendMessage(ctx context.Context, in *pb.Msg) (*pb.Msg, error) {
	val, ok := peer.FromContext(ctx)
	if ok != true {
		return nil, errors.New("Incorrect addr!")
	}

	str := val.Addr.String()
	return m.Serve(&models.Msg{Host: str, Msg: in})
}

func (m *Messenger) Send(addr string, out *pb.Msg, options ...grpc.DialOption) (*pb.Msg, error) {
	conn, err := grpc.Dial(addr, options...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	c := pb.NewConnecterClient(conn)
	var req *pb.Msg
	req, err = c.SendMessage(context.Background(), out)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (m *Messenger) Serve(value *models.Msg) (*pb.Msg, error) {
	return m.out(value)
}

func (m *Messenger) listener(port string) {
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Panicln("failed to listen: %v", err)
		return
	}
	s := grpc.NewServer(grpc.MaxConcurrentStreams(1000), grpc.MaxMsgSize(math.MaxInt32))
	pb.RegisterConnecterServer(s, m)
	reflection.Register(s)

	if err := s.Serve(lis); err != nil {
		log.Panicln("failed to serve: %v", err)
		return
	}
}
