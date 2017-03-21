package work

import (
	pb "../../connecter"
	m "../../messenger"
	"../../models"
	"errors"
	"github.com/gogo/protobuf/proto"
	"google.golang.org/grpc"
	"math/rand"
	mr "math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

type Work struct {
	status     uint8
	wtype      uint8
	clientAddr string
	sport      string
	err        error
	id         []uint64
	pack       []byte
	postman    *m.Messenger
	in         chan interface{}
	out        chan interface{}
	feed       *chan *Work
}

func Create(feed *chan *Work, postman *m.Messenger, id []uint64, clientAddr, wtype string, sport string) *Work {
	w := &Work{}
	w.id = id
	w.clientAddr = clientAddr
	w.status = 0
	w.err = nil
	w.postman = postman
	w.sport = sport
	switch wtype {
	case "pi":
		w.wtype = 0
	case "vmatrix":
		w.wtype = 1
	case "mmatrix":
		w.wtype = 2
	case "sort":
		w.wtype = 3
	}
	w.feed = feed
	rand.Seed(time.Now().Unix())
	w.in = make(chan interface{})
	w.out = make(chan interface{})
	return w
}

func (w *Work) Begin(data *[]byte) error {
	w.status = 1
	switch w.wtype {
	case 0:
		var d models.PiRequest
		if err := proto.Unmarshal(*data, &d); err != nil {
			w.err = err
		} else {
			f := make(chan uint8)
			go w.monteCarlo(&d, &f)
			<-f
			close(f)
		}
	case 1:
		var d models.VMatrixRequest
		if err := proto.Unmarshal(*data, &d); err != nil {
			w.err = err
		} else {
			f := make(chan uint8)
			go w.vmatrixMultiplication(&d, &f)
			<-f
			close(f)
		}
	case 2:
		var d models.MMatrixRequest
		if err := proto.Unmarshal(*data, &d); err != nil {
			w.err = err
		} else {
			f := make(chan uint8)
			go w.mmatrixMultiplication(&d, &f)
			<-f
			close(f)
		}
	case 3:
		var d models.SortRequest
		if err := proto.Unmarshal(*data, &d); err != nil {
			w.err = err
		} else {
			f := make(chan uint8)
			go w.sort(&d, &f)
			<-f
			close(f)
		}
	}
	data = nil
	if w.err != nil {
		w.status = 3
		defer w.close()
		return w.err
	}
	return nil
}

func (w *Work) Send(in *pb.Msg) *models.ReqMsg {
	w.in <- in
	tmp := <-w.out
	return tmp.(*models.ReqMsg)
}

func (w *Work) SendResult() {
	w.postman.Send(w.clientAddr[:strings.LastIndex(w.clientAddr, ":")]+w.sport,
		&pb.Msg{Header: []string{"result", w.GetType()}, Id: w.id, Data: w.pack},
		grpc.WithInsecure())
}

func (w *Work) SendError() {
	w.postman.Send(w.clientAddr[:strings.LastIndex(w.clientAddr, ":")]+w.sport,
		&pb.Msg{Header: []string{"error", w.GetType()}, Id: w.id, Data: []byte(w.err.Error())},
		grpc.WithInsecure())
}

func (w *Work) Remove() bool {
	if w.GetStatus() != 6 {
		w.in <- uint8(0)
		<-w.out
		return true
	}
	return false
}

func (w *Work) GetId() []uint64 {
	return w.id
}

func (w *Work) GetStatus() uint8 {
	return w.status
}

func (w *Work) GetClientAddr() string {
	return w.clientAddr
}

func (w *Work) GetPack() *[]byte {
	return &w.pack
}

func (w *Work) GetType() string {
	switch w.wtype {
	case 0:
		return "pi"
	case 1:
		return "vmatrix"
	case 2:
		return "mmatrix"
	case 3:
		return "sort"
	}
	return ""
}

func (w *Work) GetError() error {
	return w.err
}

func (w *Work) close() {
	close(w.in)
	close(w.out)
}

func (w *Work) stub() {
	for {
		select {
		case msg := <-w.in:
			switch msg.(type) {
			case uint8:
				val := msg.(uint8)
				if val == 0 {
					w.status = 6
					w.out <- uint8(0)
					return
				}
			case *models.Msg:
				w.out <- &models.ReqMsg{Err: errors.New("Over!")}
			}
		}
	}
}

func (w *Work) monteCarlo(pi *models.PiRequest, f *chan uint8) {
	defer w.close()
	var hit uint64
	mr.Seed(time.Now().Unix())
	*f <- 0
	for i := uint64(0); i < pi.Iterations; i++ {
		select {
		case msg := <-w.in:
			switch msg.(type) {
			case uint8:
				val := msg.(uint8)
				if val == 0 {
					w.status = 6
					w.out <- uint8(0)
					return
				}
			case *pb.Msg:
				w.out <- &models.ReqMsg{}
			}
		default:
			x := float64(pi.Radius) * mr.Float64()
			y := float64(pi.Radius) * mr.Float64()
			if y*y+x*x <= float64(pi.Radius*pi.Radius) {
				hit++
			}
		}
	}
	var err error
	if w.pack, err = proto.Marshal(&models.PiResponse{Hit: hit, Iterations: pi.Iterations}); err != nil {
		w.err = err
		w.status = 4
		w.SendError()
	} else {
		w.status = 2
		w.SendResult()
	}
	*w.feed <- w
	w.stub()
	return
}

func (w *Work) vmatrixMultiplication(matrix *models.VMatrixRequest, f *chan uint8) {
	defer w.close()
	result := &models.VMatrixResponse{}
	result.ResultVector = make(map[uint64]float64)
	*f <- 0
	for key, value := range matrix.MatrixRows {
		result.ResultVector[key] = 0
		for i := uint64(0); i < matrix.Size_; i++ {
			select {
			case msg := <-w.in:
				switch msg.(type) {
				case uint8:
					val := msg.(uint8)
					if val == 0 {
						w.status = 6
						w.out <- uint8(0)
						return
					}
				case *pb.Msg:
					w.out <- &models.ReqMsg{}
				}
			default:
				result.ResultVector[key] += value.Row[i] * matrix.Vector[i]
			}
		}
	}
	var err error
	if w.pack, err = proto.Marshal(result); err != nil {
		w.err = err
		w.status = 4
		w.SendError()
	} else {
		w.status = 2
		w.SendResult()
	}
	result = nil
	*w.feed <- w
	w.stub()
	return
}

func (w *Work) mmatrixMultiplication(matrix *models.MMatrixRequest, f *chan uint8) {
	defer w.close()
	logic := CreateML(matrix, w)
	go logic.handlerML()
	go func() {
		time.Sleep(time.Second * 1)
		*f <- 0
	}()
	for {
		select {
		case msg := <-w.in:
			switch msg.(type) {
			case uint8:
				val := msg.(uint8)
				if val == 0 {
					w.status = 6
					logic.Stop()
					w.out <- uint8(0)
					return
				}
			case *pb.Msg:
				logic.handleMsg(msg.(*pb.Msg))
			}
		}
	}
	return
}

func (w *Work) sort(sort *models.SortRequest, f *chan uint8) {
	defer w.close()
	logic := CreateSL(sort, w)
	go logic.handlerSL()
	go func() {
		time.Sleep(time.Second * 1)
		*f <- 0
	}()
	for {
		select {
		case msg := <-w.in:
			switch msg.(type) {
			case uint8:
				val := msg.(uint8)
				if val == 0 {
					w.status = 6
					logic.Stop()
					w.out <- uint8(0)
					return
				}
			case *pb.Msg:
				logic.handleMsg(msg.(*pb.Msg))
			}
		}
	}
	return
}

//mmatrix begin

type matrixLogic struct {
	base     *models.MMatrixRequest
	result   *models.MMatrixResponse
	tmp      *models.MMatrixResponse
	squad    *models.WorkerNetwork
	in       chan uint8
	w        *Work
	err      error
	progress uint64
	phase    uint8
	start    bool
	handled  bool
	over     bool
}

func CreateML(matrix *models.MMatrixRequest, w *Work) *matrixLogic {
	ml := &matrixLogic{}
	ml.base = matrix
	ml.result = &models.MMatrixResponse{I: matrix.I, J: matrix.J}
	ml.tmp = &models.MMatrixResponse{I: matrix.I, J: matrix.J}
	ml.result.T = make([]*models.Row, ml.base.Brows)
	ml.tmp.T = make([]*models.Row, ml.base.Brows)
	for key, _ := range ml.result.T {
		ml.result.T[key] = &models.Row{Row: make([]float64, ml.base.Bcolumns)}
		ml.tmp.T[key] = &models.Row{Row: make([]float64, ml.base.Bcolumns)}
	}
	ml.squad = &models.WorkerNetwork{WorkerMatrix: make(map[uint64]*models.WorkerLine)}
	ml.w = w
	ml.in = make(chan uint8)
	return ml
}

func (ml *matrixLogic) Stop() {
	ml.in <- uint8(0)
}

func (ml *matrixLogic) handlerML() {
	for {
		select {
		case msg := <-ml.in:
			if msg == 0 {
				close(ml.in)
				return
			}
		default:
			if ml.checkError() {
				ml.handleDefault()
				ml.checkEnd()
			}
		}
	}
}

func (ml *matrixLogic) handleMsg(msg *pb.Msg) {
	switch msg.Header[1] {
	case "start":
		if err := proto.Unmarshal(msg.Data, ml.squad); err != nil {
			ml.err = err
			ml.w.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: err}
		} else {
			ml.start = true
			ml.w.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
		}
	case "a":
		if (ml.start == true) && (ml.phase == 1) && (ml.handled == true) {
			ml.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"success"}}, Err: nil}
			if err := proto.Unmarshal(msg.Data, ml.tmp); err != nil {
				ml.err = err
				ml.handled = false
			} else {
				ml.handled = false
			}
		} else {
			ml.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"timeout"}}, Err: nil}
		}
	case "b":
		if (ml.start == true) && (ml.phase == 3 || ml.phase == 4) && (ml.handled == true) {
			ml.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"success"}}, Err: nil}
			if err := proto.Unmarshal(msg.Data, ml.tmp); err != nil {
				ml.err = err
				ml.handled = false
			} else {
				ml.handled = false
			}
		} else {
			ml.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"timeout"}}, Err: nil}
		}
	default:
		ml.w.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: errors.New("Unknown message!")}
	}
}

func (ml *matrixLogic) checkError() bool {
	if ml.over == false {
		if ml.err != nil {
			ml.w.status = 4
			ml.w.err = ml.err
			ml.w.SendError()
			*ml.w.feed <- ml.w
			ml.over = true
			return false
		}
	} else {
		return false
	}
	return true
}

func (ml *matrixLogic) checkEnd() {
	if ml.progress >= ml.base.Iter && ml.err == nil {
		var err error
		if ml.w.pack, err = proto.Marshal(ml.result); err != nil {
			ml.err = err
		} else {
			ml.w.status = 2
			ml.w.SendResult()
			ml.result = nil
			*ml.w.feed <- ml.w
			ml.over = true
		}
	}
}

func (ml *matrixLogic) handleDefault() {
	if ml.start == true && ml.handled == false && ml.over == false {
		switch ml.phase {
		case 0:
			if ml.base.J == ((ml.base.I + uint64(ml.progress)) % uint64(ml.base.Rows)) {
				var wg sync.WaitGroup
				wg.Add(1)
				go ml.additionC(ml.base.A, &wg)
				ml.err = ml.dispatchA()
				wg.Wait()
				ml.phase = 2
			} else {
				ml.phase = 1
				ml.handled = true
			}
		case 1:
			var wg sync.WaitGroup
			wg.Add(1)
			ml.additionC(ml.tmp, &wg)
			wg.Wait()
			ml.phase = 2
		case 2:
			if ml.base.I == ml.base.Rows-1 {
				var c bool
				ml.err, c = ml.sendB()
				if c {
					ml.phase = 0
					ml.progress++
				} else {
					ml.handled = true
					ml.phase = 3
				}
			} else {
				ml.handled = true
				ml.phase = 4
			}
		case 3:
			ml.overwriteB()
			ml.progress++
			ml.phase = 0
		case 4:
			ml.err, _ = ml.sendB()
			ml.overwriteB()
			ml.progress++
			ml.phase = 0
		}
	}
}

func (ml *matrixLogic) dispatchA() error {
	dbytes, err := proto.Marshal(ml.base.A)
	if err != nil {
		return err
	}
	feed := make(chan error, len(ml.squad.WorkerMatrix)-1)
	for key, value := range ml.squad.WorkerMatrix[ml.base.I].Workers {
		if uint64(key) != ml.base.J {
			go ml.sendA(&feed, value, &dbytes)
		}
	}
	flag := false
	for i := 0; i < len(ml.squad.WorkerMatrix)-1; i++ {
		if err := <-feed; err != nil {
			flag = true
		}
	}
	close(feed)
	if flag == true {
		return errors.New("Faild dispatch A matrix!")
	}
	return nil
}

func (ml *matrixLogic) sendA(back *chan error, w *models.Worker, dbytes *[]byte) {
	a := time.Now()
	for {
		val, err := ml.w.postman.Send(w.Addr,
			&pb.Msg{Header: []string{"msg", "a"}, Id: []uint64{ml.w.GetId()[0], w.Id}, Data: *dbytes},
			grpc.WithInsecure())
		if err != nil {
			*back <- err
			return
		}
		if val.Header[0] != "timeout" {
			*back <- nil
			return
		}
		if time.Now().Sub(a).Seconds() > 50 {
			*back <- errors.New("So slow!")
			return
		}
	}
}

func (ml *matrixLogic) additionC(operand *models.MMatrixResponse, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := uint64(0); i < ml.base.Brows; i++ {
		for j := uint64(0); j < ml.base.Bcolumns; j++ {
			ml.result.T[i].Row[j] += ml.mulA(operand, i, j)
		}
	}
}

func (ml *matrixLogic) mulA(operand *models.MMatrixResponse, ti, tj uint64) float64 {
	result := float64(0)
	for j, i := uint64(0), uint64(0); j < ml.base.Bcolumns && i < ml.base.Brows; j++ {
		result += operand.T[ti].Row[j] * ml.base.B.T[i].Row[tj]
		i++
	}
	return result
}

func (ml *matrixLogic) sendB() (error, bool) {
	var i uint64
	if ml.base.I != 0 {
		i = ml.base.I - 1
	} else {
		i = ml.base.Rows - 1
	}
	if i == ml.base.I {
		return nil, true
	}
	w := ml.squad.WorkerMatrix[i].Workers[ml.base.J]
	dbytes, err := proto.Marshal(ml.base.B)
	if err != nil {
		return err, false
	}
	b := time.Now()
	for {
		val, err := ml.w.postman.Send(w.Addr,
			&pb.Msg{Header: []string{"msg", "b"}, Id: []uint64{ml.w.GetId()[0], w.Id}, Data: dbytes},
			grpc.WithInsecure())
		if err != nil {
			return err, false
		}
		if val.Header[0] != "timeout" {
			return nil, false
		}
		if time.Now().Sub(b).Seconds() > 50 {
			return errors.New("So slow!"), false
		}
	}
	return nil, false
}

func (ml *matrixLogic) overwriteB() {
	for i := uint64(0); i < ml.base.Brows; i++ {
		for j := uint64(0); j < ml.base.Bcolumns; j++ {
			ml.base.B.T[i].Row[j] = ml.tmp.T[i].Row[j]
		}
	}
}

//mmatrix end

//sort begin

type sortLogic struct {
	result     *models.SortResponse
	tmp        *models.SortResponse
	doubledTmp []uint32
	squad      *models.WorkerLine
	in         chan uint8
	w          *Work
	err        error
	progress   uint64
	iter       uint64
	start      bool
	over       bool
	handled    bool
	noti       bool
}

func CreateSL(sort *models.SortRequest, w *Work) *sortLogic {
	sl := &sortLogic{}
	sl.result = &models.SortResponse{DataLine: make([]uint32, len(sort.DataLine)), Number: sort.Number}
	sl.tmp = &models.SortResponse{DataLine: make([]uint32, len(sort.DataLine)), Number: sort.Number}
	sl.result.DataLine = sort.DataLine
	sl.iter = sort.Iter
	sl.w = w
	sl.doubledTmp = make([]uint32, len(sort.DataLine)*2)
	sl.in = make(chan uint8)
	sl.squad = &models.WorkerLine{Workers: make([]*models.Worker, sort.Iter)}
	return sl
}

func (sl *sortLogic) Stop() {
	sl.in <- uint8(0)
}

func (sl *sortLogic) handlerSL() {
	for {
		select {
		case msg := <-sl.in:
			if msg == 0 {
				close(sl.in)
				return
			}
		default:
			if sl.checkError() {
				sl.handleDefault()
				sl.checkEnd()
			}
		}
	}
}

func (sl *sortLogic) handleMsg(msg *pb.Msg) {
	switch msg.Header[1] {
	case "start":
		if err := proto.Unmarshal(msg.Data, sl.squad); err != nil {
			sl.err = err
			sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: err}
		} else {
			sl.start = true
			sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{}, Err: nil}
		}
	case "odd":
		if (sl.start == true) && (sl.progress%2 == 1) && sl.handled == true {
			if err := proto.Unmarshal(msg.Data, sl.tmp); err != nil {
				sl.err = err
				sl.handled = false
				sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"error"}}, Err: err}
			} else {
				dbytes, err := proto.Marshal(sl.result)
				if err != nil {
					sl.err = err
					sl.handled = false
					sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"error"}}, Err: err}
				}
				sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"success"}, Data: dbytes}, Err: nil}
				sl.noti = true
				sl.handled = false
			}
		} else {
			sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"timeout"}}, Err: nil}
		}
	case "even":
		if (sl.start == true) && (sl.progress%2 == 0) && sl.handled == true {
			if err := proto.Unmarshal(msg.Data, sl.tmp); err != nil {
				sl.err = err
				sl.handled = false
				sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"error"}}, Err: err}
			} else {
				dbytes, err := proto.Marshal(sl.result)
				if err != nil {
					sl.err = err
					sl.handled = false
					sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"error"}}, Err: err}
				}
				sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"success"}, Data: dbytes}, Err: nil}
				sl.noti = true
				sl.handled = false
			}
		} else {
			sl.w.out <- &models.ReqMsg{Msg: &pb.Msg{Header: []string{"timeout"}}, Err: nil}
		}
	}
}

func (sl *sortLogic) checkError() bool {
	if sl.over == false {
		if sl.err != nil {
			sl.w.status = 4
			sl.w.err = sl.err
			sl.w.SendError()
			*sl.w.feed <- sl.w
			sl.over = true
			return false
		}
	} else {
		return false
	}
	return true
}

func (sl *sortLogic) checkEnd() {
	if sl.progress >= sl.iter && sl.err == nil {
		var err error
		if sl.w.pack, err = proto.Marshal(sl.result); err != nil {
			sl.err = err
		} else {
			sl.w.status = 2
			sl.w.SendResult()
			sl.result = nil
			*sl.w.feed <- sl.w
			sl.over = true
		}
	}
}

func (sl *sortLogic) handleDefault() {
	if sl.start == true && sl.handled == false && sl.over == false {
		switch sl.progress % 2 {
		case 0:
			if sl.result.Number%2 == 0 {
				var ok bool
				sl.err, ok = sl.sendBlock("even")
				if sl.err == nil {
					sl.sortLine(ok)
				}
				sl.progress++
			} else {
				if sl.noti == false {
					sl.handled = true
				} else {
					sl.sortLine(false)
					sl.noti = false
					sl.progress++
				}
			}
		case 1:
			if sl.result.Number%2 == 1 {
				var ok bool
				sl.err, ok = sl.sendBlock("odd")
				if sl.err == nil {
					sl.sortLine(ok)
				}
				sl.progress++
			} else {
				if sl.result.Number != 0 && sl.result.Number != (sl.iter-1) {
					if sl.noti == false {
						sl.handled = true
					} else {
						sl.sortLine(false)
						sl.noti = false
						sl.progress++
					}
				} else {
					sl.progress++
				}
			}
		}
	}
}

func (sl *sortLogic) sendBlock(t string) (error, bool) {
	var i uint64
	i = (sl.result.Number + 1) % sl.iter
	if i == sl.result.Number {
		return nil, true
	}
	if sl.result.Number > i {
		return nil, true
	}
	w := sl.squad.Workers[i]
	dbytes, err := proto.Marshal(sl.result)
	if err != nil {
		return err, false
	}
	b := time.Now()
	for {
		val, err := sl.w.postman.Send(w.Addr,
			&pb.Msg{Header: []string{"msg", t}, Id: []uint64{sl.w.GetId()[0], w.Id}, Data: dbytes},
			grpc.WithInsecure())
		if err != nil {
			return err, false
		}
		if val.Header[0] != "timeout" {
			if err := proto.Unmarshal(val.Data, sl.tmp); err != nil {
				return err, false
			} else {
				return nil, false
			}
		}
		if time.Now().Sub(b).Seconds() > 50 {
			return errors.New("So slow!"), false
		}
	}
	return nil, false
}

type ByMax []uint32

func (bm ByMax) Len() int { return len(bm) }
func (bm ByMax) Swap(i int, j int) {
	bm[i], bm[j] = bm[j], bm[i]
}
func (bm ByMax) Less(i int, j int) bool {
	return bm[i] < bm[j]
}

func (sl *sortLogic) sortLine(ok bool) {
	if ok {
		sort.Sort(ByMax(sl.result.DataLine))
	} else {
		size := len(sl.tmp.DataLine)
		for key, _ := range sl.tmp.DataLine {
			sl.doubledTmp[size+key] = sl.tmp.DataLine[key]
			sl.doubledTmp[key] = sl.result.DataLine[key]
		}
		sort.Sort(ByMax(sl.doubledTmp))
		if sl.result.Number > sl.tmp.Number {
			for key, _ := range sl.result.DataLine {
				sl.result.DataLine[key] = sl.doubledTmp[size+key]
			}
		} else {
			for key, _ := range sl.result.DataLine {
				sl.result.DataLine[key] = sl.doubledTmp[key]
			}
		}
	}
}

// sort end
