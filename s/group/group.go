package group

import (
	pb "../../connecter"
	m "../../messenger"
	"../../models"
	"../../primary"
	"../work"
	"errors"
	"github.com/gogo/protobuf/proto"
	"math"
	"time"
)

type Group struct {
	status   uint8
	id       uint64
	data     interface{}
	err      error
	works    map[uint64]*work.Work
	counter  int
	postman  *m.Messenger
	in       chan interface{}
	out      chan interface{}
	feedline chan *work.Work
	feed     *chan *Group
}

func Create(feed *chan *Group, postman *m.Messenger, id uint64) *Group {
	g := &Group{}
	g.feed = feed
	g.postman = postman
	g.id = id
	g.status = 0
	g.err = nil
	g.counter = 0
	g.works = make(map[uint64]*work.Work)
	g.in = make(chan interface{})
	g.out = make(chan interface{})
	g.feedline = make(chan *work.Work, 1000)
	return g
}

func (g *Group) close() {
	g.removeWorkers()
	close(g.in)
	close(g.out)
	close(g.feedline)
}

func (g *Group) getClearId() uint64 {
	for {
		id := primary.GenerateId()
		if _, ok := g.works[id]; !ok {
			return id
		}
	}
	return 0
}

func (g *Group) removeWorkers() {
	for key, val := range g.works {
		val.Remove()
		delete(g.works, key)
	}
}

func (g *Group) Begin(clients *map[string]uint64, data interface{}) error {
	g.data = data
	err := g.initWorkers(clients)
	if err != nil {
		g.status = 3
		g.err = err
		defer g.close()
		return err
	}
	go g.handler()
	err = g.begin()
	if err != nil {
		g.status = 3
		g.err = err
		return err
	}
	return nil
}

func (g *Group) GetId() uint64 {
	return g.id
}

func (g *Group) Remove() {
	if g.status != 0 && g.status != 3 && g.status != 6 {
		g.in <- uint8(1)
		<-g.out
	}
}

func (g *Group) Send(in *models.Msg) *models.ReqMsg {
	g.in <- in
	tmp := <-g.out
	return tmp.(*models.ReqMsg)
}

func (g *Group) GetStatus() uint8 {
	return g.status
}

func (g *Group) GetError() error {
	return g.err
}

func (g *Group) begin() error {
	g.in <- uint8(0)
	tmp := <-g.out
	if val, ok := tmp.(error); ok {
		return val
	}
	return nil
}

func (g *Group) handler() {
	defer g.close()
	for {
		select {
		case msg := <-g.in:
			switch msg.(type) {
			case uint8:
				switch msg.(uint8) {
				case 0:
					g.beginWork()
					if g.err != nil {
						return
					} else {
						g.status = 1
					}
				case 1:
					g.status = 6
					g.out <- uint8(0)
					return
				}
			case *models.Msg:
				g.handleMsg(msg.(*models.Msg))
			}
		case msg := <-g.feedline:
			g.handleWork(msg)
		}
	}
}

func (g *Group) handleWork(w *work.Work) {
	switch w.GetStatus() {
	case 2:
		if w.GetStatus() != 6 {
			w.Stop()
			g.counter++
			g.finalize()
		}
	case 4:
		w.Stop()
		g.handleError(w.GetError(), false)
	}
}

func (g *Group) handleMsg(msg *models.Msg) {
	switch msg.Msg.Header[0] {
	case "error":
		if val, ok := g.works[msg.Msg.Id[1]]; ok {
			val.Stop()
			g.handleError(errors.New(string(msg.Msg.Data)), true)
		} else {
			g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
		}
	case "result":
		if val, ok := g.works[msg.Msg.Id[1]]; ok {
			g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
			if val.GetStatus() != 6 {
				val.Stop()
				g.counter++
				val.SetPack(&msg.Msg.Data)
				g.finalize()
			}
		} else {
			g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
		}
	case "msg":
		g.handleMsgMsg(msg)
	}
}

func (g *Group) handleError(err error, fl bool) {
	if fl {
		g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
	}
	g.removeWorkers()
	g.status = 4
	g.err = err
	*g.feed <- g
}

func (g *Group) initWorkers(clients *map[string]uint64) error {
	var err error
	switch g.data.(type) {
	case *models.PiBase:
		err = g.initPi(clients)
	case *models.VMatrixBase:
		err = g.initVMatrix(clients)
	case *models.MMatrixBase:
		err = g.initMMatrix(clients)
	case *models.SortBase:
		err = g.initSort(clients)
	default:
		err = errors.New("Undefined type!")
	}
	return err
}

func (g *Group) initPi(clients *map[string]uint64) error {
	tmp := g.data.(*models.PiBase)
	fl := false
	for key, value := range *clients {
		for i := uint64(0); i < value; i++ {
			var workData models.PiRequest
			id := g.getClearId()
			if fl {
				workData = models.PiRequest{Radius: tmp.Radius, Iterations: tmp.Iterations / tmp.WorkerCount}
			} else {
				workData = models.PiRequest{Radius: tmp.Radius, Iterations: (tmp.Iterations / tmp.WorkerCount) + (tmp.Iterations % tmp.WorkerCount)}
				fl = true
			}
			dbytes, err := proto.Marshal(&workData)
			if err != nil {
				return err
			}
			g.works[id] = work.Create(&g.feedline, g.postman, []uint64{g.id, id}, key, &dbytes)
		}
	}
	return nil
}

func (g *Group) initVMatrix(clients *map[string]uint64) error {
	tmp := g.data.(*models.VMatrixBase)
	fl := false
	i, j := uint64(0), uint64(0)
	for key, value := range *clients {
		for k := uint64(0); k < value; k++ {
			workData := &models.VMatrixRequest{Size_: tmp.Size, Vector: tmp.Vector}
			id := g.getClearId()
			if fl {
				j += tmp.Size / tmp.WorkerCount
				workData.MatrixRows = make(map[uint64]*models.Row)
				for ; i < j; i++ {
					workData.MatrixRows[i] = &models.Row{Row: tmp.Matrix[i]}
				}
			} else {
				j += tmp.Size/tmp.WorkerCount + tmp.Size%tmp.WorkerCount
				workData.MatrixRows = make(map[uint64]*models.Row)
				for ; i < j; i++ {
					workData.MatrixRows[i] = &models.Row{Row: tmp.Matrix[i]}
				}
				fl = true
			}
			dbytes, err := proto.Marshal(workData)
			workData = nil
			if err != nil {
				return err
			}
			g.works[id] = work.Create(&g.feedline, g.postman, []uint64{g.id, id}, key, &dbytes)
		}
	}
	return nil
}

func (g *Group) initMMatrix(clients *map[string]uint64) error {
	tmp := g.data.(*models.MMatrixBase)
	var rows, columns, brows, bcolumns uint64
	block := (tmp.Size * tmp.Size) / tmp.WorkerCount
	brows = uint64(math.Sqrt(float64(block)))
	bcolumns = brows
	rows = tmp.Size / brows
	columns = rows
	tmp.Workers = &models.WorkerNetwork{WorkerMatrix: make(map[uint64]*models.WorkerLine)}
	for i := uint64(0); i < rows; i++ {
		tmp.Workers.WorkerMatrix[i] = &models.WorkerLine{Workers: make([]*models.Worker, columns)}
	}
	var i, j uint64
	for key, value := range *clients {
		for k := uint64(0); k < value; k++ {
			id := g.getClearId()
			workData := &models.MMatrixRequest{Rows: rows, Brows: brows, Bcolumns: bcolumns, Iter: uint64(math.Sqrt(float64(tmp.WorkerCount))), I: i, J: j}
			tmp.Workers.WorkerMatrix[i].Workers[j] = &models.Worker{Addr: key, Id: id}
			workData.A = &models.MMatrixResponse{T: make([]*models.Row, brows)}
			workData.B = &models.MMatrixResponse{T: make([]*models.Row, brows)}
			for ti, br, ni := i*brows, i*brows+brows, uint64(0); ti < br; ti++ {
				workData.A.T[ni] = &models.Row{Row: make([]float64, bcolumns)}
				workData.B.T[ni] = &models.Row{Row: make([]float64, bcolumns)}
				for tj, bc, nj := j*bcolumns, j*bcolumns+bcolumns, uint64(0); tj < bc; tj++ {
					workData.A.T[ni].Row[nj] = tmp.AMatrix[ti][tj]
					workData.B.T[ni].Row[nj] = tmp.BMatrix[ti][tj]
					nj++
				}
				ni++
			}
			j++
			if j >= columns {
				j = 0
				i++
			}
			dbytes, err := proto.Marshal(workData)
			workData = nil
			if err != nil {
				return err
			}
			g.works[id] = work.Create(&g.feedline, g.postman, []uint64{g.id, id}, key, &dbytes)
		}
	}
	g.data = tmp
	return nil
}

func (g *Group) initSort(clients *map[string]uint64) error {
	tmp := g.data.(*models.SortBase)
	tmp.Workers = &models.WorkerLine{Workers: make([]*models.Worker, tmp.WorkerCount)}
	k := uint64(0)
	for key, value := range *clients {
		for i := uint64(0); i < value; i++ {
			workData := &models.SortRequest{Number: k, Iter: tmp.WorkerCount, DataLine: make([]uint32, tmp.BlockSize)}
			id := g.getClearId()
			tmp.Workers.Workers[k] = &models.Worker{Addr: key, Id: id}
			for j := uint64(0); j < tmp.BlockSize; j++ {
				workData.DataLine[j] = tmp.DataLine[k*tmp.BlockSize+j]
			}
			dbytes, err := proto.Marshal(workData)
			if err != nil {
				return err
			}
			g.works[id] = work.Create(&g.feedline, g.postman, []uint64{g.id, id}, key, &dbytes)
			k++
		}
	}
	g.data = tmp
	return nil
}

func (g *Group) beginWork() {
	var err error
	switch g.data.(type) {
	case *models.PiBase:
		err = g.beginPi()
	case *models.VMatrixBase:
		err = g.beginVMatrix()
	case *models.MMatrixBase:
		err = g.beginMMatrix()
	case *models.SortBase:
		err = g.beginSort()
	}
	g.err = err
	g.out <- err
}

func (g *Group) beginPi() error {
	tmp := g.data.(*models.PiBase)
	tmp.Begin = time.Now()
	g.data = tmp
	err := g.begInitial(tmp.WorkerCount, "pi")
	return err
}

func (g *Group) beginVMatrix() error {
	tmp := g.data.(*models.VMatrixBase)
	tmp.Begin = time.Now()
	g.data = tmp
	err := g.begInitial(tmp.WorkerCount, "vmatrix")
	return err
}

func (g *Group) beginMMatrix() error {
	tmp := g.data.(*models.MMatrixBase)
	err := g.begInitial(tmp.WorkerCount, "mmatrix")
	if err != nil {
		return err
	}
	tmp.Begin = time.Now()
	var dbytes []byte
	dbytes, err = proto.Marshal(tmp.Workers)
	if err != nil {
		return err
	}
	g.data = tmp
	err = g.begStart(&dbytes, tmp.WorkerCount)
	if err != nil {
		return err
	}
	return nil
}

func (g *Group) beginSort() error {
	tmp := g.data.(*models.SortBase)
	err := g.begInitial(tmp.WorkerCount, "sort")
	if err != nil {
		return err
	}
	tmp.Begin = time.Now()
	var dbytes []byte
	dbytes, err = proto.Marshal(tmp.Workers)
	if err != nil {
		return err
	}
	g.data = tmp
	err = g.begStart(&dbytes, tmp.WorkerCount)
	if err != nil {
		return err
	}
	return nil
}

func (g *Group) begInitial(count uint64, wtype string) error {
	feed := make(chan error, count)
	for _, value := range g.works {
		go value.Begin(&feed, wtype)
	}
	flag := false
	for i := uint64(0); i < count; i++ {
		if err := <-feed; err != nil {
			flag = true
		}
	}
	close(feed)
	if flag == true {
		return errors.New("Begin faild!")
	} else {
		return nil
	}
}

func (g *Group) begStart(dbytes *[]byte, count uint64) error {
	feed := make(chan error, count)
	for _, value := range g.works {
		go g.sendStart(&feed, value, dbytes)
	}
	flag := false
	for i := uint64(0); i < count; i++ {
		if err := <-feed; err != nil {
			flag = true
		}
	}
	close(feed)
	if flag == true {
		return errors.New("Begin faild!")
	}
	return nil
}

func (g *Group) sendStart(back *chan error, w *work.Work, dbytes *[]byte) {
	_, err := w.Send(&pb.Msg{Header: []string{"msg", "start"}, Id: w.GetId(), Data: *dbytes})
	if err != nil {
		*back <- err
	} else {
		*back <- nil
	}
}

func (g *Group) handleMsgMsg(msg *models.Msg) {
	switch g.data.(type) {
	case *models.PiBase:
		g.msgPi(msg)
	case *models.VMatrixBase:
		g.msgVMatrix(msg)
	case *models.MMatrixBase:
		g.msgMMatrix(msg)
	case *models.SortBase:
		g.msgSort(msg)
	}
}

func (g *Group) msgPi(msg *models.Msg) {
	g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
}

func (g *Group) msgVMatrix(msg *models.Msg) {
	g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
}

func (g *Group) msgMMatrix(msg *models.Msg) {
	g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
}

func (g *Group) msgSort(msg *models.Msg) {
	g.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
}

func (g *Group) finalize() {
	if g.status == 1 {
		if g.counter < len(g.works) {
			return
		}
		var err error
		if g.status != 6 && g.status != 2 {
			switch g.data.(type) {
			case *models.PiBase:
				err = g.finalizePi()
			case *models.VMatrixBase:
				err = g.finalizeVMatrix()
			case *models.MMatrixBase:
				err = g.finalizeMMatrix()
			case *models.SortBase:
				err = g.finalizeSort()
			}
		}
		if err != nil {
			g.status = 4
			g.err = err
			g.removeWorkers()
		} else {
			g.status = 2
		}
		*g.feed <- g
	}
}

func (g *Group) finalizePi() error {
	tmp := g.data.(*models.PiBase)
	tmp.End = time.Now()
	hit := uint64(0)
	for _, value := range g.works {
		var singlData models.PiResponse
		if err := proto.Unmarshal(*value.GetPack(), &singlData); err != nil {
			return err
		}
		hit += singlData.Hit
	}
	tmp.Result = (float64(hit) / float64(tmp.Iterations)) * 4
	g.data = tmp
	return nil
}

func (g *Group) finalizeVMatrix() error {
	tmp := g.data.(*models.VMatrixBase)
	tmp.End = time.Now()
	for _, value := range g.works {
		var singlData models.VMatrixResponse
		if err := proto.Unmarshal(*value.GetPack(), &singlData); err != nil {
			return err
		}
		for key, value := range singlData.ResultVector {
			tmp.ResultVector[key] = value
		}
	}
	g.data = tmp
	return nil
}

func (g *Group) finalizeMMatrix() error {
	tmp := g.data.(*models.MMatrixBase)
	tmp.End = time.Now()
	var brows, bcolumns uint64
	block := (tmp.Size * tmp.Size) / tmp.WorkerCount
	if math.Sqrt(float64(block))*math.Sqrt(float64(block)) != float64(block) {
		brows = tmp.Size
		bcolumns = uint64(1)
		return errors.New("Timeout!")
	} else {
		brows = uint64(math.Sqrt(float64(block)))
		bcolumns = brows
	}
	for _, value := range g.works {
		var singlData models.MMatrixResponse
		if err := proto.Unmarshal(*value.GetPack(), &singlData); err != nil {
			return err
		}
		for ti, br, ni := singlData.I*brows, singlData.I*brows+brows, uint64(0); ti < br; ti++ {
			for tj, bc, nj := singlData.J*bcolumns, singlData.J*bcolumns+bcolumns, uint64(0); tj < bc; tj++ {
				tmp.CMatrix[ti][tj] = singlData.T[ni].Row[nj]
				nj++
			}
			ni++
		}
	}
	g.data = tmp
	return nil
}

func (g *Group) finalizeSort() error {
	tmp := g.data.(*models.SortBase)
	tmp.End = time.Now()
	for _, value := range g.works {
		var singlData models.SortResponse
		if err := proto.Unmarshal(*value.GetPack(), &singlData); err != nil {
			return err
		}
		for i := uint64(0); i < tmp.BlockSize; i++ {
			tmp.SortLine[singlData.Number*tmp.BlockSize+i] = singlData.DataLine[i]
		}
	}
	g.data = tmp
	return nil
}

func (g *Group) Show() (interface{}, error) {
	if g.status == 6 {
		return g.data, nil
	} else {
		return nil, errors.New("Is not ready!")
	}
}
