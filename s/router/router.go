package router

import (
	pb "../../connecter"
	m "../../messenger"
	"../../models"
	"../../primary"
	"../group"
	ss "../subnetScanner"
	"errors"
	"google.golang.org/grpc"
	"log"
	"os"
	"runtime"
	"runtime/debug"
)

const (
	cport = ":55555"
	sport = ":55559"
)

type Router struct {
	scanner  *ss.SubnetScanner
	postman  *m.Messenger
	active   map[uint64]*group.Group
	finished map[uint64]*group.Group
	failds   map[uint64]*group.Group
	in       chan interface{}
	out      chan interface{}
	feedline chan *group.Group
}

func Create() *Router {
	r := &Router{}
	r.postman = m.Create(r.handle, sport)
	r.scanner = ss.Create(r.postman, cport)
	r.active = make(map[uint64]*group.Group)
	r.finished = make(map[uint64]*group.Group)
	r.failds = make(map[uint64]*group.Group)
	r.in = make(chan interface{})
	r.out = make(chan interface{})
	r.feedline = make(chan *group.Group, 1000)
	return r
}

func (r *Router) Exit(fl uint64) {
	r.in <- &models.ControlMsg{Code: 0, Id: fl}
	<-r.out
}

func (r *Router) Clear(fl uint64) {
	r.in <- &models.ControlMsg{Code: 1, Id: fl}
	<-r.out
}

func (r *Router) Scan() {
	r.in <- &models.ControlMsg{Code: 2}
	<-r.out
}

func (r *Router) GetClients() *map[string]string {
	r.in <- &models.ControlMsg{Code: 3}
	tmp := <-r.out
	return tmp.(*map[string]string)
}

func (r *Router) GetGroups() *[][]uint64 {
	r.in <- &models.ControlMsg{Code: 4}
	tmp := <-r.out
	return tmp.(*[][]uint64)
}

func (r *Router) CreateGroup(clients *map[string]uint64, data interface{}) (uint64, error) {
	w := &models.Wrapper{Clients: clients, Data: data}
	r.in <- &models.ControlMsg{Code: 5, Data: w}
	tmp := <-r.out
	switch tmp.(type) {
	case uint64:
		return tmp.(uint64), nil
	case error:
		return 0, tmp.(error)
	}
	return 0, nil
}

func (r *Router) RemoveGroup(id uint64) error {
	r.in <- &models.ControlMsg{Code: 6, Id: id}
	tmp := <-r.out
	switch tmp.(type) {
	case uint8:
		return nil
	case error:
		return tmp.(error)
	}
	return nil
}

func (r *Router) ShowGroup(id uint64) (interface{}, error) {
	r.in <- &models.ControlMsg{Code: 7, Id: id}
	tmp := <-r.out
	if val, ok := tmp.(error); ok {
		return nil, val
	}
	return tmp, nil
}

func (r *Router) getClearId() uint64 {
	for {
		id := primary.GenerateId()
		if _, ok := r.active[id]; !ok {
			return id
		}
	}
	return 0
}

func (r *Router) close() {
	for key, val := range r.active {
		val.Remove()
		delete(r.active, key)
	}
	for key, _ := range r.finished {
		delete(r.finished, key)
	}
	for key, _ := range r.failds {
		delete(r.failds, key)
	}
	close(r.feedline)
}

func (r *Router) handle(value *models.Msg) (*pb.Msg, error) {
	r.in <- value
	tmp := <-r.out
	return tmp.(*models.ReqMsg).Msg, tmp.(*models.ReqMsg).Err
}

func (r *Router) Handler() {
	defer r.close()
	for {
		select {
		case msg := <-r.in:
			switch msg.(type) {
			case *models.Msg:
				r.handleMsg(msg.(*models.Msg))
			case *models.ControlMsg:
				r.handleControlMsg(msg.(*models.ControlMsg))
			}
		case msg := <-r.feedline:
			r.handleGroup(msg)
		}
	}
}

func (r *Router) handleMsg(msg *models.Msg) {
	if val, ok := r.active[msg.Msg.Id[0]]; ok {
		r.out <- val.Send(msg)
	} else {
		r.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: errors.New("Does not exist!")}
	}
}

func (r *Router) handleGroup(this *group.Group) {
	switch this.GetStatus() {
	case 2:
		this.Remove()
		r.finished[this.GetId()] = this
		delete(r.active, this.GetId())
		log.Println(this.GetId(), ":Group: finished!")
	case 4:
		this.Remove()
		r.failds[this.GetId()] = this
		delete(r.active, this.GetId())
		log.Println(this.GetId(), ":Group: faild!")
	}
}

func (r *Router) handleControlMsg(msg *models.ControlMsg) {
	switch msg.Code {
	case 0:
		r.handleExit(msg)
	case 1:
		r.handleClear(msg)
	case 2:
		r.handleScan()
	case 3:
		r.handleClients()
	case 4:
		r.handleGroups()
	case 5:
		r.handleCreate(msg)
	case 6:
		r.handleRemove(msg)
	case 7:
		r.handleShow(msg)
	}
}

func (r *Router) handleExit(msg *models.ControlMsg) {
	if msg.Id == 1 {
		for _, value := range r.active {
			value.Remove()
		}
		list := r.scanner.GetClients()
		for key, _ := range list {
			r.postman.Send(key, &pb.Msg{Header: []string{"exit"}}, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(time.Millisecond*100), grpc.WithBlock())
		}
	}
	r.out <- uint8(0)
	os.Exit(0)
}

func (r *Router) handleClear(msg *models.ControlMsg) {
	if msg.Id == 1 {
		list := r.scanner.GetClients()
		for key, _ := range list {
			r.postman.Send(key, &pb.Msg{Header: []string{"clear"}}, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(time.Millisecond*100), grpc.WithBlock())
		}
	}
	for key, _ := range r.finished {
		delete(r.finished, key)
	}
	for key, _ := range r.failds {
		delete(r.failds, key)
	}
	runtime.GC()
	debug.FreeOSMemory()
	r.out <- uint8(0)
}

func (r *Router) handleScan() {
	r.scanner.Scan()
	r.out <- uint8(0)
}

func (r *Router) handleClients() {
	clients := r.scanner.GetClients()
	r.out <- &clients
}

func (r *Router) handleGroups() {
	tmp := make([][]uint64, 3)
	tmp[0] = make([]uint64, len(r.active))
	i := 0
	for key, _ := range r.active {
		tmp[0][i] = key
		i++
	}
	tmp[1] = make([]uint64, len(r.finished))
	i = 0
	for key, _ := range r.finished {
		tmp[1][i] = key
		i++
	}
	tmp[2] = make([]uint64, len(r.failds))
	i = 0
	for key, _ := range r.failds {
		tmp[2][i] = key
		i++
	}
	r.out <- &tmp
}

func (r *Router) handleCreate(msg *models.ControlMsg) {
	id := r.getClearId()
	current := group.Create(&r.feedline, r.postman, id)
	err := current.Begin(msg.Data.(*models.Wrapper).Clients, msg.Data.(*models.Wrapper).Data)
	if err != nil {
		r.out <- err
	} else {
		r.active[id] = current
		r.out <- id
	}
}

func (r *Router) handleRemove(msg *models.ControlMsg) {
	if val, ok := r.active[msg.Id]; ok {
		val.Remove()
		delete(r.active, msg.Id)
		r.out <- uint8(0)
	}
	r.out <- errors.New("Does not exist!")
}

func (r *Router) handleShow(msg *models.ControlMsg) {
	if val, ok := r.finished[msg.Id]; ok {
		tmp, err := val.Show()
		if err != nil {
			r.out <- err
		} else {
			r.out <- tmp
		}
		return
	}
	if val, ok := r.failds[msg.Id]; ok {
		r.out <- val.GetError()
		return
	}
	if _, ok := r.active[msg.Id]; ok {
		r.out <- errors.New("Is not ready!")
		return
	}
	r.out <- errors.New("Does not exist!")
	return
}
