package work

import (
	pb "../../connecter"
	m "../../messenger"
	"../../models"
	"google.golang.org/grpc"
	"time"
)

type Work struct {
	status     uint8
	clientAddr string
	err        error
	id         []uint64
	pack       *[]byte
	postman    *m.Messenger
	in         chan interface{}
	out        chan interface{}
	data       *[]byte
	end        bool
	feed       *chan *Work
}

func Create(feed *chan *Work, postman *m.Messenger, id []uint64, clientAddr string, data *[]byte) *Work {
	w := &Work{}
	w.status = 0
	w.clientAddr = clientAddr
	w.postman = postman
	w.data = data
	w.id = id
	w.err = nil
	w.end = false
	w.in = make(chan interface{})
	w.out = make(chan interface{})
	w.feed = feed
	return w
}

func (w *Work) close() {
	if w.end != true {
		<-w.in
		w.out <- uint8(1)
	}
	close(w.in)
	close(w.out)
}

func (w *Work) Begin(back *chan error, wtype string) {
	_, err := w.postman.Send(w.clientAddr, &pb.Msg{Header: []string{"create", wtype}, Id: w.id, Data: *w.data}, grpc.WithInsecure())
	if err != nil {
		w.status = 3
		*back <- err
		w.end = true
		defer w.close()
	} else {
		w.status = 1
		*back <- nil
		go w.handler()
	}
	w.data = nil
}

func (w *Work) Send(in *pb.Msg) (*pb.Msg, error) {
	w.in <- in
	tmp := <-w.out
	return tmp.(*models.ReqMsg).Msg, tmp.(*models.ReqMsg).Err
}

func (w *Work) Remove() error {
	if w.status == 6 || w.status == 0 || w.status == 3 {
		return nil
	} else {
		w.in <- uint8(0)
		tmp := <-w.out
		if val, ok := tmp.(error); ok {
			return val
		}
		return nil
	}

}

func (w *Work) Stop() {
	if w.status == 6 || w.status == 0 || w.status == 3 {
		return
	} else {
		w.in <- uint8(2)
		<-w.out
	}
}

func (w *Work) GetStatus() uint8 {
	return w.status
}

func (w *Work) GetError() error {
	return w.err
}

func (w *Work) GetPack() *[]byte {
	return w.pack
}

func (w *Work) GetId() []uint64 {
	return w.id
}

func (w *Work) SetPack(pack *[]byte) {
	w.pack = pack
}

func (w *Work) stat() {
	for {
		time.Sleep(time.Second * 5)
		w.in <- uint8(1)
		ans := <-w.out
		if ans.(uint8) == 1 {
			return
		}
	}
}

func (w *Work) handler() {
	defer w.close()
	go w.stat()
	for {
		select {
		case msg := <-w.in:
			switch msg.(type) {
			case uint8:
				switch msg.(uint8) {
				case 0:
					var err error
					if w.status == 1 {
						_, err = w.postman.Send(w.clientAddr, &pb.Msg{Header: []string{"remove"}, Id: w.id}, grpc.WithInsecure())
					}
					w.status = 6
					w.out <- err
					return
				case 1:
					if w.status != 1 {
						w.out <- uint8(1)
						w.end = true
					} else {
						w.out <- uint8(0)
						w.getStatus()
					}
				case 2:
					w.status = 6
					w.out <- uint8(0)
					return
				}
			case *pb.Msg:
				val, err := w.postman.Send(w.clientAddr, msg.(*pb.Msg), grpc.WithInsecure())
				w.out <- &models.ReqMsg{Msg: val, Err: err}
			}
		}
	}
}

func (w *Work) getStatus() {
	val, err := w.postman.Send(w.clientAddr, &pb.Msg{Header: []string{"status"}, Id: w.id}, grpc.WithInsecure())
	if err != nil {
		w.err = err
		w.status = 4
		*w.feed <- w
		return
	} else {
		switch val.Header[0] {
		case "Working!":
			return
		case "Finished!":
			w.pack = &val.Data
			w.status = 2
			*w.feed <- w
			return
		}
	}

}
