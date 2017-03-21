package router

import (
	pb "../../connecter"
	m "../../messenger"
	"../../models"
	w "../work"
	"errors"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"time"
)

const (
	cport = ":55555"
	sport = ":55559"
)

type Router struct {
	postman  *m.Messenger
	works    map[uint64]*w.Work
	finished map[uint64]*[]byte
	errors   map[uint64]error
	in       chan *models.Msg
	out      chan *models.ReqMsg
	feedline chan *w.Work
}

func Create() *Router {
	r := &Router{}
	r.postman = m.Create(r.handle, cport)
	r.in = make(chan *models.Msg)
	r.out = make(chan *models.ReqMsg)
	r.feedline = make(chan *w.Work, 1000)
	r.works = make(map[uint64]*w.Work)
	r.finished = make(map[uint64]*[]byte)
	r.errors = make(map[uint64]error)
	return r
}

func (r *Router) close() {
	for key, val := range r.works {
		val.Remove()
		delete(r.works, key)
	}
	close(r.feedline)
	for key, _ := range r.finished {
		delete(r.finished, key)
	}
	for key, _ := range r.errors {
		delete(r.errors, key)
	}
}

func (r *Router) check(id uint64) bool {
	if _, ok := r.works[id]; ok {
		return true
	}
	if _, ok := r.finished[id]; ok {
		return true
	}
	if _, ok := r.errors[id]; ok {
		return true
	}
	return false
}

func (r *Router) handle(value *models.Msg) (*pb.Msg, error) {
	r.in <- value
	tmp := <-r.out
	return tmp.Msg, tmp.Err
}

func (r *Router) Handler() {
	defer r.close()
	for {
		select {
		case msg := <-r.in:
			r.handleMsg(msg)
		case msg := <-r.feedline:
			r.handleWork(msg)
		}
	}
}

func (r *Router) handleMsg(this *models.Msg) {
	switch this.Msg.Header[0] {
	case "create":
		r.msgCreate(this)
	case "status":
		r.msgStatus(this)
	case "remove":
		r.msgRemove(this)
	case "msg":
		r.msgMsg(this)
	case "clear":
		r.msgClear(this)
	case "exit":
		r.msgExit(this)
	case "introduce":
		r.msgIntroduce(this)
	default:
		r.out <- &models.ReqMsg{}
	}
}

func (r *Router) handleWork(this *w.Work) {
	switch this.GetStatus() {
	case 2:
		r.workFinish(this)
	case 4:
		r.workError(this)
	}
}

func (r *Router) msgCreate(this *models.Msg) {
	if r.check(this.Msg.Id[1]) {
		r.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: errors.New("Id already taken!")}
		log.Println(this.Host + ": Create: Id already taken!")
	} else {
		current := w.Create(&r.feedline, r.postman, this.Msg.Id, this.Host, this.Msg.Header[1], sport)
		r.works[this.Msg.Id[1]] = current
		if err := current.Begin(&this.Msg.Data); err != nil {
			delete(r.works, this.Msg.Id[1])
			r.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: err}
			log.Println(this.Host+": Create:", err)
		} else {
			r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Success"}}, Err: nil}
			log.Println(this.Host+": Create:", this.Msg.Id[1])
		}
	}
}

func (r *Router) msgStatus(this *models.Msg) {
	if _, ok := r.works[this.Msg.Id[1]]; ok {
		r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Working!"}}, Err: nil}
		log.Println(this.Host+":", this.Msg.Id[1], ":Status: Working!")
	} else {
		if val, ok := r.finished[this.Msg.Id[1]]; ok {
			r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Finished!"}, Data: *val}, Err: nil}
			log.Println(this.Host+":", this.Msg.Id[1], ":Status: Finished!")
		} else {
			if val, ok := r.errors[this.Msg.Id[1]]; ok {
				r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Error!"}}, Err: val}
				log.Println(this.Host+":", this.Msg.Id[1], ":Status: Error!")
			} else {
				r.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: errors.New("Does not exist!")}
				log.Println(this.Host+":", this.Msg.Id[1], ":Status: Does not exist!")
			}
		}
	}
}

func (r *Router) msgRemove(this *models.Msg) {
	if val, ok := r.works[this.Msg.Id[1]]; ok {
		val.Remove()
		r.errors[this.Msg.Id[1]] = val.GetError()
		delete(r.works, this.Msg.Id[1])
		r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Success"}}, Err: nil}
		log.Println(this.Host+":", this.Msg.Id[1], ":Remove: Success!")
	} else {
		r.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: errors.New("Does not exist!")}
		log.Println(this.Host+":", this.Msg.Id[1], ":Remove: Does not exist!")
	}
}

func (r *Router) msgMsg(this *models.Msg) {
	if val, ok := r.works[this.Msg.Id[1]]; ok {
		r.out <- val.Send(this.Msg)
	} else {
		r.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: errors.New("Does not exist!")}
	}
}

func (r *Router) msgClear(this *models.Msg) {
	for key, _ := range r.finished {
		r.finished[key] = nil
		delete(r.finished, key)
	}
	for key, _ := range r.errors {
		delete(r.errors, key)
	}
	r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Success"}}, Err: nil}
	log.Println(this.Host + ":Clear")
	runtime.GC()
	debug.FreeOSMemory()
}

func (r *Router) msgExit(this *models.Msg) {
	log.Println(this.Host + ":Exit")
	r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Success"}}, Err: nil}
	time.Sleep(time.Second)
	os.Exit(0)
}

func (r *Router) msgIntroduce(this *models.Msg) {
	name, err := os.Hostname()
	if err != nil {
		r.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: err}
		log.Println(this.Host+":Introduce:", err)
	} else {
		r.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"Hello"}, Data: []byte(name)}, Err: nil}
		log.Println(this.Host + ":Introduce: Success")
	}
}

func (r *Router) workFinish(this *w.Work) {
	if this.Remove() != true {
		return
	}
	r.finished[this.GetId()[1]] = this.GetPack()
	delete(r.works, this.GetId()[1])
	log.Println(this.GetId()[1], ": Finished!")
}

func (r *Router) workError(this *w.Work) {
	if this.Remove() != true {
		return
	}
	r.errors[this.GetId()[1]] = this.GetError()
	delete(r.works, this.GetId()[1])
	log.Println(this.GetId()[1], ": Error!")
}
