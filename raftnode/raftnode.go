package raftnode

import (
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cs733-iitb/cluster"
	mockcluster "github.com/cs733-iitb/cluster/mock"
	"github.com/cs733-iitb/log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"
)

const (
	CONFIG_FILE_NAME        = "raft_config.json"
	NO_OF_SERVERS           = 5
	RANDOM_TIMEOUT_INTERVAL = 250
	debugFlag               = false
)

var rafts []*RaftNode
var IS_CLUSTER_UP = false
var PERSISTENT_STATE_FILE_DIR string

// Config struct for retrieving coniguration information
type Config struct {
	Cluster             []NetConfig // Information about all servers, including this.
	NodeCluster         *mockcluster.MockCluster
	Clientports         []int  // Ports on which clients will listen
	Id                  int    // this node's id. One of the cluster's entries should match.
	LogDir              string // Log file directory for this node
	PersistenceStateDir string // Persistence State file directory name
	ElectionTimeout     int    // election timeout value in millis
	HeartbeatTimeout    int    // HeartBeat timeout value in millis
	InboxSize           int    // Inbox size on channel
	OutboxSize          int    // Outbox size on channel
}

// This struct holds network information
type NetConfig struct {
	Id   int
	Host string
	Port int
}

// Struct for storing CurrentTerm and VotedFor for raftnodes
type Persistence struct {
	Id          int // this node's id. One of the cluster's entries should match.
	CurrentTerm int
	VotedFor    int
}

type Node interface {
	// Client's message to Raft node
	Append([]byte)
	// A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() <-chan Commit
	//A channel for Node.
	ShutdownChannel() <-chan int
	//A channel for listening on client events
	EventChannel() chan Event
	// Last known committed index in the log. This could be -1 until the system stabilizes.
	CommittedIndex() int
	// Returns the data at a log index, or an error.
	Get(index int) (error, []byte)
	// Node's id
	Id() int
	// Id of leader. -1 if unknown
	LeaderId() int
	// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
	// Restore cluster, log, timers, start all channels and deploy raftnode
	Restore()
}

// Main raftNode struct
type RaftNode struct { // implements Node interface
	id           int
	eventCh      chan Event
	commitCh     chan *Commit
	timeoutCh    chan Event
	shutdownCh   chan int
	waitShutdown *sync.WaitGroup

	sm *StateMachine

	clusterNode  cluster.Server
	mNodeCluster *mockcluster.MockCluster

	electionTimeout          int
	heartbeatTimeout         int
	timer                    *time.Timer
	heartbeatTimer           *time.Ticker
	log                      *log.Log
	PersistenceStateFileName string // Persistence State file name for this node
	LogDirectory             string // Log directory for this node
}

// Return ClusterNodeObject for raftNode
func (rn *RaftNode) ClusterNode() cluster.Server {
	return rn.clusterNode
}

// Return MockCluster for raftNode
func (rn *RaftNode) Cluster() *mockcluster.MockCluster {
	return rn.mNodeCluster
}

// Return CommitChannel for raftNode
func (rn *RaftNode) CommitChannel() chan *Commit {
	return rn.commitCh
}

// Return ShutdownChannel for raftNode
func (rn *RaftNode) ShutdownChannel() chan int {
	return rn.shutdownCh
}

// Return EventChannel for raftNode
func (rn *RaftNode) EventChannel() chan Event {
	return rn.eventCh
}

// Return TimeoutChannel for raftNode
func (rn *RaftNode) TimeoutChannel() chan Event {
	return rn.timeoutCh
}

// Send Timeout structure to EventCh
func (rn *RaftNode) Timeout() {
	rn.timeoutCh <- Timeout{}
}

// Send Append structure to EventCh
func (rn *RaftNode) Append(data interface{}) {
	rn.eventCh <- Append{Data: data}
}

// Node's Id
func (rn *RaftNode) Id() int {
	return rn.id
}

// Leader Id
func (rn *RaftNode) LeaderId() int {
	return rn.sm.LeaderId
}

// Last known committed index in the log.
func (rn *RaftNode) CommittedIndex() int {
	return rn.sm.CommitIndex
}

/*
* Desc: Starts a goroutine for the raftnode server
 */
func (rn *RaftNode) DeployRaftNode() {
	go rn.processEvents()
}

/*
* Desc: Returns the state of statemachine inside raftnode
 */
func (rn *RaftNode) getRaftNodeState() string {
	return reflect.TypeOf(rn.sm.State).Name()
}

/*
* Desc: Returns the state of statemachine inside raftnode
 */
func (rn *RaftNode) GetPersistenceStateFileName() string {
	return rn.PersistenceStateFileName
}

// Signal to shut down all goroutines, stop sockets, flush log and close it, stop timers.
func (rn *RaftNode) Shutdown() {
	rn.timer.Stop()
	rn.waitShutdown.Add(1)
	close(rn.ShutdownChannel())
	rn.waitShutdown.Wait()
}

// Shutdown each raftnode iteratively
func ShutdownSystem(rafts []*RaftNode) {
	fmt.Printf("\nShutting down all rafts !")
	for _, r := range rafts {
		r.Shutdown()
	}
}

func (rn *RaftNode) processEvents() {
	for {
		var ev Event
		select {
		case ev = <-rn.EventChannel():
			actions := rn.sm.processEvent(ev)
			rn.doActions(actions)
		case ev = <-rn.TimeoutChannel():
			actions := rn.sm.processEvent(ev)
			rn.doActions(actions)
		case ev := <-rn.ClusterNode().Inbox():
			printRecvEvent(rn, ev)
			actions := rn.sm.processEvent(ev.Msg)
			rn.doActions(actions)
		case <-rn.timer.C:
			rn.Timeout()
		case _, ok := <-rn.ShutdownChannel():
			if !ok { // If channel closed, return from function
				close(rn.CommitChannel())
				close(rn.EventChannel())
				close(rn.timeoutCh)
				rn.clusterNode.Close()

				rn.log.Close()
				rn.sm = nil
				rn.waitShutdown.Done()
				return
			}
		default:
		}
	}
}

/*
* Desc: Performs action present in action array
 */
func (rn *RaftNode) doActions(actions []Action) {
	for _, action := range actions {

		switch action.(type) {
		case Alarm:
			alarm := action.(Alarm)
			rn.timer.Reset(time.Duration(alarm.T) * time.Millisecond)
			printEvent(rn, alarm)
		case Send:
			send := action.(Send)
			rn.clusterNode.Outbox() <- &cluster.Envelope{Pid: send.from, Msg: send.event}
			printEvent(rn, send)
		case StateStore:
			stateStore := action.(StateStore)
			persistenceStateFileName := rn.GetPersistenceStateFileName()
			persistenceObj, err := readPersistenceStateFile(persistenceStateFileName)
			if err != nil {
				fmt.Println("Error in reading PersistenceStateFile !", err)
			}
			persistenceObj.CurrentTerm = stateStore.CurrentTerm
			persistenceObj.VotedFor = stateStore.VotedFor
			err = writePersistenceObjectToFile(persistenceObj, persistenceStateFileName)
			if err != nil {
				fmt.Printf("Error in writing to a %s file", persistenceStateFileName)
			}
			printEvent(rn, stateStore)
		case Commit:
			commit := action.(Commit)
			rn.CommitChannel() <- &commit
			printEvent(rn, commit)
		}
	}
}

// Signal to shut down all goroutines, stop sockets, flush log and close it, stop timers.
func (rn *RaftNode) Restore(config *Config) {
	logEntry := make([]LogEntry, 0)
	logEntry = append(logEntry, LogEntry{Term: 0})
	peers := make([]int, NO_OF_SERVERS)
	voteMap := make(map[int]string, NO_OF_SERVERS)
	nextIndex := []int{1, 1, 1, 1, 1}
	matchIndex := make([]int, 5)

	//TODO: Take config as argument to this function
	//config, _ := ReadConfigFile(CONFIG_FILE_NAME)

	clusterconfig := cluster.Config{
		Peers:      []cluster.PeerConfig{},
		InboxSize:  config.InboxSize,
		OutboxSize: config.OutboxSize,
	}

	//Generate peers array for statemachine
	for index, clusterNode := range config.Cluster {
		peers[index] = clusterNode.Id
		clusterconfig.Peers = append(clusterconfig.Peers, cluster.PeerConfig{Id: clusterNode.Id, Address: fmt.Sprint(clusterNode.Host, ":", clusterNode.Port)})
	}

	persistenceObject, err := readPersistenceStateFile(rn.PersistenceStateFileName)
	if err != nil {
		fmt.Println("Error in reading PersistenceStateFile !")
		return
	}

	//Read the log corresponding to raftnode and update the statemachine's log
	logDir := rn.LogDirectory
	lg, err := log.Open(logDir)
	if err != nil {
		fmt.Println("Error in log read !")
		return
	}
	//Register LogEntry struct with log package
	lg.RegisterSampleEntry(LogEntry{})

	statemachine := &StateMachine{
		Id:               rn.Id(),
		Log:              lg,
		CurrentTerm:      persistenceObject.CurrentTerm,
		VotedFor:         persistenceObject.VotedFor,
		CommitIndex:      0,
		N:                NO_OF_SERVERS,
		LeaderId:         -1,
		State:            Follower{},
		PeerIDs:          peers,
		VoteMap:          voteMap,
		NextIndex:        nextIndex,
		MatchIndex:       matchIndex,
		ElectionTimeout:  rn.electionTimeout,
		HeartbeatTimeout: config.HeartbeatTimeout,
	}

	rn.eventCh = make(chan Event, 1000)
	rn.timeoutCh = make(chan Event, 1000)
	rn.commitCh = make(chan *Commit, 1000)
	rn.shutdownCh = make(chan int)
	var wg sync.WaitGroup
	rn.waitShutdown = &wg
	rn.sm = statemachine
	rn.log = lg

	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(RANDOM_TIMEOUT_INTERVAL)
	rn.timer = time.NewTimer(time.Duration(rn.electionTimeout+randomNumber) * time.Millisecond)

	rn.clusterNode, err = cluster.New(rn.Id(), clusterconfig)
	return
}

// Creates the RaftNode object
func New(myId int, config *Config) *RaftNode {
	if !IS_CLUSTER_UP {
		IS_CLUSTER_UP = true
		Register()
		PERSISTENT_STATE_FILE_DIR = config.PersistenceStateDir
		//Clear Log base directory only if running on single system
		//cleanup(config.LogDir)
	}
	logEntry := make([]LogEntry, 0)
	logEntry = append(logEntry, LogEntry{Term: 0})
	peers := make([]int, NO_OF_SERVERS)
	voteMap := make(map[int]string, NO_OF_SERVERS)
	nextIndex := []int{1, 1, 1, 1, 1}
	matchIndex := make([]int, 5)
	var raft RaftNode

	clusterconfig := cluster.Config{
		Peers:      []cluster.PeerConfig{},
		InboxSize:  config.InboxSize,
		OutboxSize: config.OutboxSize,
	}

	//Generate peers array for statemachine
	for index, clusterNode := range config.Cluster {
		clusterconfig.Peers = append(clusterconfig.Peers, cluster.PeerConfig{Id: clusterNode.Id, Address: fmt.Sprint(clusterNode.Host, ":", clusterNode.Port)})
		peers[index] = clusterNode.Id
	}

	//persistenceObject := getPersistenceObjectForId(persistenceObjArray, myId)
	persistenceStateFileName := config.PersistenceStateDir + "/RaftNode" + strconv.Itoa(myId) + ".json"
	persistenceObject, err := readPersistenceStateFile(persistenceStateFileName)
	if err != nil {
		fmt.Println("Error in reading PersistenceStateFile !", persistenceStateFileName)
		return &raft
	}

	//Read the log corresponding to raftnode and update the statemachine's log
	logDir := config.LogDir + "/RaftNode" + strconv.Itoa(myId)
	lg, err := log.Open(logDir)
	if err != nil {
		fmt.Println("Error in log read !")
		return &raft
	}
	//Register LogEntry struct with log package
	lg.RegisterSampleEntry(LogEntry{})
	//Appending initial dummy entry
	lg.Append(LogEntry{Term: 0, Data: []byte("Dummy entry")})

	statemachine := &StateMachine{
		Id:               myId,
		Log:              lg,
		CurrentTerm:      persistenceObject.CurrentTerm,
		VotedFor:         persistenceObject.VotedFor,
		CommitIndex:      0,
		N:                NO_OF_SERVERS,
		LeaderId:         -1,
		State:            Follower{},
		PeerIDs:          peers,
		VoteMap:          voteMap,
		NextIndex:        nextIndex,
		MatchIndex:       matchIndex,
		ElectionTimeout:  config.ElectionTimeout,
		HeartbeatTimeout: config.HeartbeatTimeout,
	}

	evChan := make(chan Event, 1000)
	TimeoutChan := make(chan Event, 1000)
	CommitChan := make(chan *Commit, 1000)
	ShutdownChan := make(chan int)
	var wg sync.WaitGroup

	raft = RaftNode{
		id:                       statemachine.Id,
		eventCh:                  evChan,
		commitCh:                 CommitChan,
		timeoutCh:                TimeoutChan,
		sm:                       statemachine,
		electionTimeout:          config.ElectionTimeout,
		heartbeatTimeout:         config.HeartbeatTimeout,
		log:                      lg,
		shutdownCh:               ShutdownChan,
		PersistenceStateFileName: persistenceStateFileName,
		LogDirectory:             logDir,
		waitShutdown:             &wg,
	}

	raft.clusterNode, err = cluster.New(myId, clusterconfig)
	if err != nil {
		fmt.Println("Error in creating server for node :", myId)
	}
	rand.Seed(time.Now().UnixNano())
	randomNumber := rand.Intn(RANDOM_TIMEOUT_INTERVAL)
	raft.timer = time.NewTimer(time.Duration(config.ElectionTimeout+randomNumber) * time.Millisecond)

	return &raft
}

//Removes the log base directory
func Cleanup(dir string) {
	os.RemoveAll(dir)
	resetPersistenceStateDir()
}

/*
*
* Desc: Resets the PersistenceStateFile of corresponding raftnode to initial values as
* 		CurrentTerm = 0
*		VotedFor = -1
*
 */
func (rn *RaftNode) resetPersistenceStateFile() {

	persistenceStateFileName := rn.GetPersistenceStateFileName()
	persistenceObj, err := readPersistenceStateFile(persistenceStateFileName)
	if err != nil {
		fmt.Println("Error in reading PersistenceStateFile !")
	}
	persistenceObj.CurrentTerm = 0
	persistenceObj.VotedFor = -1
	err = writePersistenceObjectToFile(persistenceObj, persistenceStateFileName)
	if err != nil {
		fmt.Printf("Error in writing to a %s file", persistenceStateFileName)
	}
}

/*
*
* Desc: Resets the PersistenceStateFile of all raftnodes to initial values as
* 		CurrentTerm = 0
*		VotedFor = -1
*
 */
func resetPersistenceStateDir() {
	persistenceObj := new(Persistence)
	persistenceObj.CurrentTerm = 0
	persistenceObj.VotedFor = -1
	startId := 100
	diff := 100
	for i := 0; i < NO_OF_SERVERS; i++ {
		id := startId + diff*i
		persistenceObj.Id = id
		fileName := PERSISTENT_STATE_FILE_DIR + "/RaftNode" + strconv.Itoa(id) + ".json"
		err := writePersistenceObjectToFile(persistenceObj, fileName)
		if err != nil {
			fmt.Printf("Error in writing to a %s file", fileName)
		}
	}
}

/*
*
* Desc: Writes a Persistent object array to a Persistent state file
*
 */
func writePersistenceObjectToFile(persistenceObj *Persistence, persistenceFileName string) (err error) {
	var f *os.File
	if f, err = os.Create(persistenceFileName); err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if err = enc.Encode(persistenceObj); err != nil {
		return err
	}
	return nil
}

/*
*
* Desc: Reads a Persistent state file into Persistent object array
*
 */
func readPersistenceStateFile(persistentFileName string) (persistence *Persistence, err error) {

	persistenceObj := new(Persistence)
	var f *os.File
	if f, err = os.Open(persistentFileName); err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	if err = dec.Decode(&persistenceObj); err != nil {
		return nil, err
	}
	return persistenceObj, nil
}

/*
*
* Desc: Reads the configuration json file and returns a Config object after decoding the json file
*
 */
func ReadConfigFile(configFileName interface{}) (config *Config, err error) {
	var cfg Config
	var ok bool
	var configFile string
	if configFile, ok = configFileName.(string); ok {
		var f *os.File
		if f, err = os.Open(configFile); err != nil {
			return nil, err
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		if err = dec.Decode(&cfg); err != nil {
			return nil, err
		}
	} else {
		return nil, errors.New("Expected a configuration.json file name")
	}
	return &cfg, nil
}

func printRecvEvent(rn *RaftNode, event interface{}) {
	if debugFlag {
		ev := event.(*cluster.Envelope)
		fmt.Printf("\nNODE:%d(%-9s) %d <== %d Event : %-25s %+v", rn.Id(), reflect.TypeOf(rn.sm.State).Name(), rn.Id(), ev.Pid, reflect.TypeOf(ev.Msg).Name(), ev.Msg)
	}
}

func printEvent(rn *RaftNode, event interface{}) {
	if debugFlag {
		switch event.(type) {
		case Send:
			ev := event.(Send)
			fmt.Printf("\nNODE:%d(%-9s) %d ==> %d Event : %-25T %+v", rn.Id(), reflect.TypeOf(rn.sm.State).Name(), rn.Id(), ev.from, ev.event, ev.event)
		case Alarm:
			alarm := event.(Alarm)
			fmt.Printf("\nNODE:%d(%-9s) Event : %-25s %+v", rn.Id(), rn.getRaftNodeState(), reflect.TypeOf(alarm).Name(), alarm)
		case StateStore:
			stateStore := event.(StateStore)
			fmt.Printf("\nNODE:%d(%-9s) Event : %-25s %+v", rn.Id(), rn.getRaftNodeState(), reflect.TypeOf(stateStore).Name(), stateStore)
		case Commit:
			commit := event.(Commit)
			fmt.Printf("\nNODE:%d(%-9s) Event : %-25s %+v", rn.Id(), rn.getRaftNodeState(), reflect.TypeOf(commit).Name(), commit)
		}
	}

}

func Register() {
	gob.Register(VoteReq{})
	gob.Register(VoteResp{})
	gob.Register(StateStore{})
	gob.Register(AppendEntriesReq{})
	gob.Register(AppendEntriesResp{})
	gob.Register(LogEntry{})
}
