package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	filesystem "github.com/kedareab/cs733/assignment4/filesystem/fs"
	raft "github.com/kedareab/cs733/assignment4/raftnode"
	"time"
	"encoding/gob"
	"sync"
)

var crlf = []byte{'\r', '\n'}
var clientHandlersGlobal []*ClientHandler
var configGlobal *raft.Config

const (
	CONFIG_FILE_NAME = "config.json"
)

// Struct for client handler
type ClientHandler struct {
	rn 		*raft.RaftNode			//RaftNode for this Client Handler
	ClientPort      int				//Port on which client connects to client handler
	RequestMap 	map[int]chan *filesystem.Msg	//Map for request Id and commit Msg
	RequestCounter 	int				//Incremental counter for requests
	fs 		*filesystem.FS			//Filesystem object for client handler
	HostAddress	string				//Host address of server
	ReqMapLock      *sync.RWMutex			//Lock for accessing RequestMap Object
	Exitwaitgroup 	*sync.WaitGroup			// Waitgroup for Server to exit
}

type Request struct {
	Id 		int
	ServerId 	int
	ReqMsg 		*filesystem.Msg
}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

// Creates the new Client handler object and returns it
func New(index int, config *raft.Config) (ch *ClientHandler) {
	Register()
	ch = &ClientHandler{
		RequestMap   : make(map[int]chan *filesystem.Msg),
		rn: raft.New(config.Cluster[index].Id,config),
		ClientPort: config.Clientports[index],
		fs: &filesystem.FS{Dir: make(map[string]*filesystem.FileInfo, 1000), Gversion:0},
		RequestCounter:0,
		HostAddress:config.Cluster[index].Host,
		ReqMapLock: &sync.RWMutex{},
		Exitwaitgroup: &sync.WaitGroup{},
	}

	//Add client handler to global list
	clientHandlersGlobal = append(clientHandlersGlobal, ch)
	configGlobal = config
	return ch
}


func (ch *ClientHandler) run() {
	// Create a waitgroup which waits untill the rafnode does not shut down
	ch.Exitwaitgroup.Add(2)

	// Start the go routine of rafnode
	ch.rn.DeployRaftNode()
	// Start a goroutine that listens continuously on rafnode's commit channel
	go ch.ListenCommitActions()

	tcpaddr, err := net.ResolveTCPAddr("tcp", ch.HostAddress+ ":" +strconv.Itoa(ch.ClientPort))
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)

	go func () {
		for {
			tcp_conn, err := tcp_acceptor.AcceptTCP()
			check(err)
			go ch.serve(tcp_conn)				// Serve a request
		}
		ch.Exitwaitgroup.Done()
	}()
}

// Calls the run method of client handler and adds the wait on waitgroup object
func (ch *ClientHandler) Start() {
	ch.run();
	ch.Exitwaitgroup.Wait()
}


// Serve request for each client connected to client handler
func (ch *ClientHandler) serve(conn *net.TCPConn) {
	reader := bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr := filesystem.GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &filesystem.Msg{Kind: 'M'})
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &filesystem.Msg{Kind: 'M'})) {
				conn.Close()
				break
			}
		}
		waitChannel := make(chan *filesystem.Msg, 0)
		reqId := ch.AddToRequestMap(waitChannel)
		req := Request{Id:reqId,ServerId:ch.rn.Id(),ReqMsg:msg}
		ch.rn.Append(req)


		// Wait for replication to happen
		select {
		case response := <-waitChannel:
			if !reply(conn, response) {
				conn.Close()
				return
			}
		case  <- time.After(100*time.Second) :
		// Connection timed out
			reply(conn, &filesystem.Msg{Kind:'I'})
			conn.Close()
			return
		}
		//Remove wait channel from map
		ch.RemoveFromRequestMap(reqId)
		close(waitChannel)

	}
}

func (ch *ClientHandler) ListenCommitActions() {
	for {
		select {
		case commit, ok := <- ch.rn.CommitChannel() :
			if ok {

				var response *filesystem.Msg

				reqObj := commit.Data.(Request)

				if commit.Err == nil {
					response = ch.fs.ProcessMsg(reqObj.ReqMsg)
				} else {
					id, _ := strconv.Atoi(commit.Err.Error())
					//If no leader elected yet, try again with same raftnode
					if id == -1 {
						id = ch.rn.Id()
					}
					response = &filesystem.Msg{Kind:'R', RedirectURL:GetRedirectURLFromId(id)}
				}

				// Reply only if the client has requested this server
				if reqObj.ServerId == ch.rn.Id() {
					waitCh := ch.GetFromRequestMap(reqObj.Id)
					waitCh <- response
				}
			} else {
				ch.Exitwaitgroup.Done()
				return
			}
		default:

		}
	}
}

// Shutdown the client handler
func (ch *ClientHandler) Shutdown() {
	ch.rn.Shutdown()
}

//Returns the host:port for the cliendhandler with given Id, required during error redirect
func GetRedirectURLFromId(id int) string {
	var redirectURL string
	for i,cl := range configGlobal.Cluster {
		if cl.Id == id {
			redirectURL = cl.Host+":"+strconv.Itoa(configGlobal.Clientports[i])
		}
	}
	return redirectURL
}

// Adds the wait channel for request to the request map of client handler
func (ch *ClientHandler) AddToRequestMap(waitCh chan *filesystem.Msg) int {
	ch.ReqMapLock.Lock()
	ch.RequestCounter++
	ch.RequestMap[ch.RequestCounter] = waitCh
	ch.ReqMapLock.Unlock()
	return ch.RequestCounter
}

// Removes the wait channel from request map
func (ch *ClientHandler) RemoveFromRequestMap(requestId int) int {
	ch.ReqMapLock.Lock()
	_, ok := ch.RequestMap[requestId]
	if ok == true {
		delete(ch.RequestMap, requestId)
	} else {
		fmt.Printf("\nDelete Unsuccessful :", requestId)
	}
	ch.ReqMapLock.Unlock()
	return requestId
}

// Returns the wait channel corresponding to given request id
func (ch *ClientHandler) GetFromRequestMap(requestId int) chan  *filesystem.Msg{
	ch.ReqMapLock.RLock()
	waitCh := ch.RequestMap[requestId]
	ch.ReqMapLock.RUnlock()
	return waitCh
}

// Provide message reply to the conn object
func reply(conn *net.TCPConn, msg *filesystem.Msg) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	case 'R':
		resp = "ERR_REDIRECT " + msg.RedirectURL
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func Register() {
	gob.Register(Request{})
	gob.Register(filesystem.Msg{})
}