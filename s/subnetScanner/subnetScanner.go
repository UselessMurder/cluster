package subnetScanner

import (
	pb "../../connecter"
	m "../../messenger"
	"../../models"
	"encoding/binary"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

type SubnetScanner struct {
	clients map[string]string
	postman *m.Messenger
	cport   string
	line    chan *models.Aborigine
}

func Create(postman *m.Messenger, cport string) *SubnetScanner {
	ss := &SubnetScanner{}
	ss.clients = make(map[string]string)
	ss.postman = postman
	ss.cport = cport
	return ss
}

func (ss *SubnetScanner) GetClients() map[string]string {
	return ss.clients
}

func (ss *SubnetScanner) listener() {
	for msg := range ss.line {
		ss.clients[msg.Addr] = msg.Name
	}
}

func (ss *SubnetScanner) Scan() {
	addrs := ss.getIpList()
	for k := range ss.clients {
		delete(ss.clients, k)
	}
	ss.line = make(chan *models.Aborigine, 100)
	var wg sync.WaitGroup
	for _, addr := range addrs {
		wg.Add(1)
		go ss.scanSubnet(addr, &wg)
	}
	go ss.listener()
	wg.Wait()
	close(ss.line)
}

func (ss *SubnetScanner) scanSubnet(ip *net.IPNet, wg *sync.WaitGroup) {
	defer wg.Done()
	mask := ip.Mask
	Ip := binary.BigEndian.Uint32(ip.IP[12:])
	if Ip == 2130706433 {
		//ignore 127.0.0.1
		return
	} else {
		var sub, broad [4]byte
		for i := 0; i < 4; i++ {
			sub[i] = ip.IP[12+i] & mask[i]
			broad[i] = (sub[i]) | (255 - mask[i])
		}
		begin := binary.BigEndian.Uint32(sub[:])
		end := binary.BigEndian.Uint32(broad[:])
		var wgg sync.WaitGroup
		i := 0
		for begin++; begin <= end; begin++ {
			if i < 10 {
				wgg.Add(1)
				bs := make([]byte, 4)
				binary.LittleEndian.PutUint32(bs, begin)
				go ss.checkIP(net.IPv4(bs[3], bs[2], bs[1], bs[0]).String(), &wgg)
				i++
			} else {
				i = 0
				wgg.Wait()
			}
		}
		wgg.Wait()
	}
	return
}

func (ss *SubnetScanner) checkIP(addr string, wg *sync.WaitGroup) {
	defer wg.Done()
	val, err := ss.postman.Send(addr+ss.cport, &pb.Msg{Header: []string{"introduce"}, Data: nil}, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithTimeout(time.Millisecond*100), grpc.WithBlock())
	if err == nil {
		ss.line <- &models.Aborigine{Addr: addr + ss.cport, Name: string(val.Data)}
	}
	return
}

func (ss *SubnetScanner) getIpList() map[int]*net.IPNet {
	ifaces, err := net.Interfaces()
	ips := make(map[int]*net.IPNet)
	if err != nil {
		log.Println("Could not get ifaces ", err)
		return ips
	}
	t := 0
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			log.Println("Could not get ip ", err)
			continue
		}
		for _, addr := range addrs {
			if v, ok := addr.(*net.IPNet); ok {
				if v.IP.To4() != nil {

					ips[t] = v
					t++
				}
			}
		}
	}
	return ips
}
