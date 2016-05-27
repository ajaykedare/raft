package raftnode

import (
	"errors"
	"fmt"
	"github.com/cs733-iitb/log"
	"math/rand"
	"sort"
	"strconv"
)

type StateMachine struct {
	Id 		 int 		// ID of state machine
	Log              *log.Log	// Stores the logs
	CurrentTerm      int		// Current term of state machine
	VotedFor         int		// LeaderId of voted for
	CommitIndex      int		// Index of last commit of state machine
	N                int 		// number of servers
	LeaderId         int 		// Last known leader
	State            StateInterface	// State of statemachine: Follower, Candidate or Leader
	PeerIDs          []int		// Id's of Peer statemachines
	VoteMap          map[int]string	// Votemap for statemachine
	NextIndex        []int		// NextIndex pointing to next LogIndex
	MatchIndex       []int		// MatchIndex for each peer's Log
	ElectionTimeout  int		// ElectionTimeoutValue
	HeartbeatTimeout int		//HearbeatTimeout value
}

type Follower struct{}
type Leader struct{}
type Candidate struct{}
type StateInterface interface{}

type Timeout struct{}

type Event interface{}
type Action interface{}

type Send struct {
	from  int
	event Event
}

type Commit struct {
	Index int
	Data  interface{}
	Err   error
}

type Append struct {
	Data interface{}
}

type Alarm struct {
	T int
}

type StateStore struct {
	CurrentTerm int
	VotedFor    int
}

type LogEntry struct {
	Term int
	Data interface{}
}

type VoteReq struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type VoteResp struct {
	Term        int
	VoteGranted bool
	VoterId     int
}

type AppendEntriesReq struct {
	Term              int
	LeaderId          int
	LastLogIndex      int
	LastLogTerm       int
	LogEntries        []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesResp struct {
	FromId         int
	Term           int
	Success        bool
	CountOfEntries int
	LastLogIndex   int
}


func (s *StateMachine) processEvent(ev Event) []Action {
	result := make([]Action, 0)
	switch s.State.(type) {
	case Follower:
		result = s.processFollowerEvent(ev)
	case Candidate:
		result = s.processCandidateEvent(ev)
	case Leader:
		result = s.processLeaderEvent(ev)
	default:
		//
	}
	return result
}

func (s *StateMachine) clearVoteMap() {
	s.VoteMap = make(map[int]string, NO_OF_SERVERS)
}

//Returns the LogEntry present at lastIndex location
func (s *StateMachine) GetLastLogEntry() *LogEntry {
	d, err := s.Log.Get(s.Log.GetLastIndex())
	if err != nil {
		fmt.Printf("\n panic Error : %v \n", s.Log.GetLastIndex())
		panic(err)
	}
	logEntry := d.(LogEntry)
	return &logEntry
}

//Returns LogEntry present at given index
func (s *StateMachine) GetLogEntryAtIndex(index int) *LogEntry {
	d, err := s.Log.Get(int64(index))
	if err != nil {
		panic(err)
	}
	logEntry := d.(LogEntry)
	return &logEntry
}

//Returns the length of Log
func (s *StateMachine) GetLogLength() int {
	return int(s.Log.GetLastIndex()) + 1
}

//Returns the last log index in Log
func (s *StateMachine) GetLastIndex() int {
	return int(s.Log.GetLastIndex())
}

//Returns the array of LogEntry from given index till end of Log
func (s *StateMachine) GetNewLogEntriesFromIndex(index int) []LogEntry {
	logEntries := make([]LogEntry, 0)
	for ; index <= int(s.Log.GetLastIndex()); index++ {
		logEntries = append(logEntries, *s.GetLogEntryAtIndex(index))
	}

	return logEntries
}


func (s *StateMachine) processFollowerEvent(ev Event) []Action {
	action := make([]Action, 0)
	switch ev.(type) {
	case VoteReq:
		msg := ev.(VoteReq)
		voteGrantedSuccess := false
		isTermUpdated := false
		//We are behind, vote yes and update the term

		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			//Check if candidate log is at least as up-to-date as receiver's log
			if msg.LastLogTerm > s.GetLastLogEntry().Term || msg.LastLogTerm == s.GetLastLogEntry().Term && msg.LastLogIndex >= s.GetLastIndex() {
				s.VotedFor = msg.CandidateId
				voteGrantedSuccess = true
			}
			isTermUpdated = true
		}

		//If voted for this term already, check if request is from same candidate and vote yes again to same candidate
		if s.CurrentTerm == msg.Term && s.VotedFor == msg.CandidateId {
			voteGrantedSuccess = true
		}

		if isTermUpdated {
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}
		if voteGrantedSuccess {
			//reset the timer and VoteGranted true
			action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
			action = append(action, Send{from: msg.CandidateId, event: VoteResp{Term: s.CurrentTerm, VoteGranted: true, VoterId: s.Id}})
		} else {
			action = append(action, Send{from: msg.CandidateId, event: VoteResp{Term: s.CurrentTerm, VoteGranted: false, VoterId: s.Id}})
		}
	case VoteResp:
		msg := ev.(VoteResp)
		//If received the msg with higher term, update the term and votedfor and send StateStore event
		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
			//reset the timer
			action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
		}
	case Append:
		msg := ev.(Append)
		action = append(action, Commit{Index: s.GetLastIndex() + 1, Data: msg.Data, Err: errors.New(strconv.Itoa(s.LeaderId))})
	case Timeout:
		s.CurrentTerm++
		//Voting for self
		s.VotedFor = s.Id
		s.VoteMap[s.Id] = "yes"
		action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		s.State = Candidate{}
		//reset the timer
		action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
		//Start the election in candidate mode and send Vote Req to all peers
		var lastLogIndex int
		var lastLogTerm int
		if s.GetLogLength() > 0 {
			lastLogIndex = s.GetLastIndex()
			lastLogTerm = s.GetLastLogEntry().Term
		} else {
			lastLogIndex = -1
			lastLogTerm = -1
		}
		for i := 0; i < len(s.PeerIDs); i++ {
			if s.Id != s.PeerIDs[i] {
				action = append(action, Send{from: s.PeerIDs[i], event: VoteReq{Term: s.CurrentTerm, CandidateId: s.Id, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}})
			}
		}

	case AppendEntriesReq:
		msg := ev.(AppendEntriesReq)

		//reset the timer
		action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})

		appendEntriesSuccess := false
		isTermUpdated := false
		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			isTermUpdated = true
		}

		if s.CurrentTerm == msg.Term {
			if s.GetLastIndex() < msg.LastLogIndex {
				appendEntriesSuccess = false
			} else {
				if s.GetLogLength() > 0 && s.GetLogEntryAtIndex(msg.LastLogIndex).Term != msg.LastLogTerm {
					appendEntriesSuccess = false
				} else {
					//Set Append success to true for valid LogEntries
					appendEntriesSuccess = true
					s.LeaderId = msg.LeaderId
					if s.GetLogLength() > msg.LastLogIndex {
						//Truncate the extra entries
						s.Log.TruncateToEnd(int64(msg.LastLogIndex + 1))
						//Currently add the log entries one by one
						for _, logEntry := range msg.LogEntries {
							s.Log.Append(logEntry)
						}

					}
					if s.GetLogLength() > 0 {
						lastCommitIndex := min(s.CommitIndex, msg.LastLogIndex+len(msg.LogEntries))
						lastCommitIndexNew := min(msg.LeaderCommitIndex, s.GetLastIndex())
						if msg.LeaderCommitIndex > lastCommitIndex {

							//all the new uncommited entries merge and commit it data
							for i := lastCommitIndex + 1; i <= lastCommitIndexNew; i++ {
								//TODO:Use Commit{index,data,} struct
								action = append(action, Commit{Index: i, Data: s.GetLogEntryAtIndex(i).Data, Err: nil})
							}
						}
						s.CommitIndex = lastCommitIndexNew
					}
				}
			}
		}
		if isTermUpdated {
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}
		if appendEntriesSuccess {
			action = append(action, Send{from: msg.LeaderId, event: AppendEntriesResp{FromId: s.Id, Term: s.CurrentTerm, Success: true, CountOfEntries: len(msg.LogEntries), LastLogIndex: msg.LastLogIndex}})
		} else {
			action = append(action, Send{from: msg.LeaderId, event: AppendEntriesResp{FromId: s.Id, Term: s.CurrentTerm, Success: false, CountOfEntries: len(msg.LogEntries), LastLogIndex: msg.LastLogIndex}})
		}
	case AppendEntriesResp:
		msg := ev.(AppendEntriesResp)
		//If received the msg with higher term, update the term and votedfor and send StateStore event
		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}
		//reset the timer
		action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})

	default:
		fmt.Printf("Event not recognised")
	}

	return action
}

func (s *StateMachine) processCandidateEvent(ev Event) []Action {

	action := make([]Action, 0)

	switch ev.(type) {
	case VoteReq:
		msg := ev.(VoteReq)
		voteGrantedSuccess := false
		isTermUpdated := false

		//We are behind, Change to follower state and vote yes and update the term
		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.State = Follower{}
			s.VotedFor = -1
			//Check if candidate log is at least as up-to-date as receiver's log
			if msg.LastLogTerm > s.GetLastLogEntry().Term || msg.LastLogTerm == s.GetLastLogEntry().Term && msg.LastLogIndex >= s.GetLastIndex() {
				s.VotedFor = msg.CandidateId
				voteGrantedSuccess = true
			}
			isTermUpdated = true
		}

		if isTermUpdated {
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}
		if voteGrantedSuccess {
			action = append(action, Send{from: msg.CandidateId, event: VoteResp{Term: s.CurrentTerm, VoteGranted: true, VoterId: s.Id}})
		} else {
			action = append(action, Send{from: msg.CandidateId, event: VoteResp{Term: s.CurrentTerm, VoteGranted: false, VoterId: s.Id}})
		}

	case VoteResp:
		msg := ev.(VoteResp)

		//Only when VoteResp is for current election Term
		if msg.Term == s.CurrentTerm {
			majority := (NO_OF_SERVERS + 1) / 2
			if msg.VoteGranted {
				s.VoteMap[msg.VoterId] = "yes"
			} else {
				s.VoteMap[msg.VoterId] = "no"
			}
			var yesCount int
			var noCount int

			for _, vote := range s.VoteMap {
				if vote == "yes" {
					yesCount++
				} else if vote == "no" {
					noCount++
				}

			}
			//If we have majority of yes, then change to Leader state and send heartbeat requests
			if yesCount == majority {
				s.State = Leader{}
				s.LeaderId = s.Id

				//Send and Alarm event
				action = append(action, Alarm{T: s.HeartbeatTimeout})

				var lastLogIndex int
				var lastLogTerm int
				logEntry := make([]LogEntry, 0)
				if s.GetLogLength() > 0 {
					lastLogIndex = s.GetLastIndex()
					lastLogTerm = s.GetLastLogEntry().Term
				} else {
					lastLogIndex = -1
					lastLogTerm = -1
				}
				s.NextIndex = make([]int, NO_OF_SERVERS)
				s.MatchIndex = make([]int, NO_OF_SERVERS)

				//Send the heartbeats to all the peers and also update the NextIndex for each server to server's log length
				for i := 0; i < len(s.PeerIDs); i++ {
					s.NextIndex[i] = s.GetLogLength()
					if s.Id != s.PeerIDs[i] {
						action = append(action, Send{from: s.PeerIDs[i], event: AppendEntriesReq{Term: s.CurrentTerm, LeaderId: s.LeaderId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm, LogEntries: logEntry, LeaderCommitIndex: s.CommitIndex}})
					}
				}

			} else if noCount == majority {
				s.State = Follower{}
				//Send and Alarm event
				action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
			}

		}
	case Append:
		msg := ev.(Append)
		action = append(action, Commit{Index: s.GetLogLength(), Data: msg.Data, Err: errors.New(strconv.Itoa(s.LeaderId))})
	case Timeout:
		//Update the term and start a new election for next term. State will remain as Candidate
		s.CurrentTerm++
		s.VotedFor = s.Id
		//Clear the map for new election
		s.clearVoteMap()
		action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		s.State = Candidate{}
		action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
		//Start the election in candidate mode and send Vote Req to all peers
		var lastLogIndex int
		var lastLogTerm int

		if s.GetLogLength() > 0 {
			lastLogIndex = s.GetLastIndex()
			lastLogTerm = s.GetLastLogEntry().Term
		} else {
			lastLogIndex = -1
			lastLogTerm = -1
		}
		for i := 0; i < len(s.PeerIDs); i++ {
			if s.Id != s.PeerIDs[i] {
				action = append(action, Send{from: s.PeerIDs[i], event: VoteReq{Term: s.CurrentTerm, CandidateId: s.Id, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm}})
			}
		}

	case AppendEntriesReq:
		msg := ev.(AppendEntriesReq)

		//reset the timer
		action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})

		appendEntriesSuccess := false
		isTermUpdated := false

		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			isTermUpdated = true
			//Change the leaderID to greater Term request
			s.LeaderId = msg.LeaderId
			//Change to follower
			s.State = Follower{}
		}

		if s.CurrentTerm == msg.Term {
			if s.GetLastIndex() < msg.LastLogIndex {
				appendEntriesSuccess = false
			} else {
				if s.GetLogLength() > 0 && s.GetLogEntryAtIndex(msg.LastLogIndex).Term != msg.LastLogTerm {
					appendEntriesSuccess = false
				} else {
					//Set Append success to true for valid LogEntries
					appendEntriesSuccess = true
					s.LeaderId = msg.LeaderId
					if s.GetLogLength() > msg.LastLogIndex {
						s.Log.TruncateToEnd(int64(msg.LastLogIndex + 1))

						//Add all the log entries one by one
						for _, logEntry := range msg.LogEntries {
							s.Log.Append(logEntry)
						}

					}

					if s.GetLogLength() > 0 {
						lastCommitIndex := min(s.CommitIndex, msg.LastLogIndex+len(msg.LogEntries))
						lastCommitIndexNew := min(msg.LeaderCommitIndex, s.GetLastIndex())
						if msg.LeaderCommitIndex > lastCommitIndex {
							//commit the entries

							//all the new uncommited entries merge and commit its data
							for i := lastCommitIndex + 1; i <= lastCommitIndexNew; i++ {
								action = append(action, Commit{Index: i, Data: s.GetLogEntryAtIndex(i).Data, Err: nil})
							}

							s.CommitIndex = lastCommitIndexNew
						}
					}

				}
			}
		}
		if isTermUpdated {
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}
		if appendEntriesSuccess {
			action = append(action, Send{from: msg.LeaderId, event: AppendEntriesResp{FromId: s.Id, Term: s.CurrentTerm, Success: true, CountOfEntries: len(msg.LogEntries), LastLogIndex: msg.LastLogIndex}})
		} else {
			action = append(action, Send{from: msg.LeaderId, event: AppendEntriesResp{FromId: s.Id, Term: s.CurrentTerm, Success: false, CountOfEntries: len(msg.LogEntries), LastLogIndex: msg.LastLogIndex}})
		}
	case AppendEntriesResp:
		msg := ev.(AppendEntriesResp)
		//If received the msg with higher term then change the state to Follower and update the term and votedfor and send StateStore event
		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
			s.State = Follower{}
			s.clearVoteMap()
		}
		//reset the timer
		action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
	default:
		fmt.Printf("Event not recognised")
	}
	return action
}

func (s *StateMachine) processLeaderEvent(ev Event) []Action {
	action := make([]Action, 0)
	switch ev.(type) {
	case VoteReq:
		msg := ev.(VoteReq)
		voteGrantedSuccess := false
		isTermUpdated := false

		//We are behind, Change to follower state and vote yes and update the term
		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.State = Follower{}
			s.VotedFor = -1
			//Check if candidate log is at least as up-to-date as receiver's log
			if msg.LastLogTerm > s.GetLastLogEntry().Term || msg.LastLogTerm == s.GetLastLogEntry().Term && msg.LastLogIndex >= s.GetLastIndex() {
				s.VotedFor = msg.CandidateId
				voteGrantedSuccess = true
			}
			isTermUpdated = true
		}

		if isTermUpdated {
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}
		if voteGrantedSuccess {
			action = append(action, Send{from: msg.CandidateId, event: VoteResp{Term: s.CurrentTerm, VoteGranted: true, VoterId: s.Id}})
		} else {
			action = append(action, Send{from: msg.CandidateId, event: VoteResp{Term: s.CurrentTerm, VoteGranted: false, VoterId: s.Id}})
		}
	case VoteResp:
		//We can drop this event, since we have received the vote response means it is certainly for our initial vote request
		// and we have already had majority and a leader now, we can ignore this

	case Append:
		msg := ev.(Append)

		//Append data to own log
		s.Log.Append(LogEntry{Term: s.CurrentTerm, Data: msg.Data})

		//set next index and match index
		s.NextIndex = make([]int, 5)
		s.MatchIndex = make([]int, 5)
		for i := 0; i <= 4; i++ {
			s.NextIndex[i] = s.GetLogLength()
			s.MatchIndex[i] = 0
		}
		logEntry := make([]LogEntry, 0)
		logEntry = append(logEntry, LogEntry{Term: s.CurrentTerm, Data: msg.Data})

		//send append entry request to all peers
		for i := 0; i < len(s.PeerIDs); i++ {
			s.NextIndex[i] = s.GetLogLength()
			if s.Id != s.PeerIDs[i] {
				action = append(action, Send{from: s.PeerIDs[i], event: AppendEntriesReq{Term: s.CurrentTerm, LeaderId: s.LeaderId, LastLogIndex: s.GetLogLength() - 2, LastLogTerm: s.GetLogEntryAtIndex(s.GetLogLength() - 2).Term, LogEntries: logEntry, LeaderCommitIndex: s.CommitIndex}})
			}
		}
	case Timeout:
		//Reset the timer
		action = append(action, Alarm{T: s.HeartbeatTimeout})

		var lastLogIndex int
		var lastLogTerm int
		logEntry := make([]LogEntry, 0)
		//if(s.GetLogLength()>0) {
		lastLogIndex = s.GetLastIndex()
		lastLogTerm = s.GetLastLogEntry().Term
		// } else {
		// 	lastLogIndex=0
		// 	lastLogTerm=0
		// }
		s.NextIndex = make([]int, NO_OF_SERVERS)
		s.MatchIndex = make([]int, NO_OF_SERVERS)
		//Send the heartbeats to all the peers and also update the NextIndex for each server to server's log length
		for i := 0; i < len(s.PeerIDs); i++ {
			s.NextIndex[i] = s.GetLogLength()
			if s.Id != s.PeerIDs[i] {
				action = append(action, Send{from: s.PeerIDs[i], event: AppendEntriesReq{Term: s.CurrentTerm, LeaderId: s.LeaderId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm, LogEntries: logEntry, LeaderCommitIndex: s.CommitIndex}})
			}
		}

	case AppendEntriesReq:
		msg := ev.(AppendEntriesReq)
		appendEntriesSuccess := false
		isTermUpdated := false

		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			isTermUpdated = true
			//Change the leaderID to greater Term request
			s.LeaderId = msg.LeaderId
			//Change to follower
			s.State = Follower{}
			//reset the timer
			action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
		}
		if s.CurrentTerm == msg.Term {
			if s.GetLastIndex() < msg.LastLogIndex {
				appendEntriesSuccess = false
			} else {
				if s.GetLogLength() > 0 && s.GetLogEntryAtIndex(msg.LastLogIndex).Term != msg.LastLogTerm {
					appendEntriesSuccess = false
				} else {
					//Set Append success to true for valid LogEntries
					appendEntriesSuccess = true
					s.LeaderId = msg.LeaderId
					if s.GetLogLength() > msg.LastLogIndex {
						//Truncate the extra entries if any
						s.Log.TruncateToEnd(int64(msg.LastLogIndex + 1))
						//Add all the log entries one by one
						for _, logEntry := range msg.LogEntries {
							s.Log.Append(logEntry)
						}
					}
					if s.GetLogLength() > 0 {
						lastCommitIndex := min(s.CommitIndex, msg.LastLogIndex+len(msg.LogEntries))
						lastCommitIndexNew := min(msg.LeaderCommitIndex, s.GetLastIndex())
						if msg.LeaderCommitIndex > lastCommitIndex {
							//commit the entries
							//all the new uncommited entries merge and commit its data
							for i := lastCommitIndex + 1; i <= lastCommitIndexNew; i++ {
								action = append(action, Commit{Index: i, Data: s.GetLogEntryAtIndex(i).Data, Err: nil})
							}

							s.CommitIndex = lastCommitIndexNew
						}
					}
				}
			}
		}
		if isTermUpdated {
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}
		if appendEntriesSuccess {
			action = append(action, Send{from: msg.LeaderId, event: AppendEntriesResp{FromId: s.Id, Term: s.CurrentTerm, Success: true, CountOfEntries: len(msg.LogEntries), LastLogIndex: msg.LastLogIndex}})
		} else {
			action = append(action, Send{from: msg.LeaderId, event: AppendEntriesResp{FromId: s.Id, Term: s.CurrentTerm, Success: false, CountOfEntries: len(msg.LogEntries), LastLogIndex: msg.LastLogIndex}})
		}
	case AppendEntriesResp:
		msg := ev.(AppendEntriesResp)
		id := 0
		for i,peerId := range s.PeerIDs{
			if peerId == msg.FromId{
				id = i
			}
		}
		isTermUpdated := false
		if s.CurrentTerm < msg.Term {
			s.CurrentTerm = msg.Term
			s.VotedFor = -1
			s.State = Follower{}
			isTermUpdated = true
			//reset the timer
			action = append(action, Alarm{T: s.ElectionTimeout + rand.Intn(RANDOM_TIMEOUT_INTERVAL)})
		} else {
			//We got false in success, decrement the NextIndex and retry the AppendEntriesReq
			if !msg.Success {
				if s.NextIndex[id] > 0 {
					s.NextIndex[id] = s.NextIndex[id] - 1
				} else {
					s.NextIndex[id] = 0
				}

				//gather the information to be sent to the AppenEntriesReq object
				var lastLogIndex int
				if s.GetLogLength() > 1 {
					lastLogIndex = msg.LastLogIndex - 1
				} else {
					lastLogIndex = msg.LastLogIndex
				}
				lastLogTerm := s.GetLogEntryAtIndex(lastLogIndex).Term
				newLogEntries := s.GetNewLogEntriesFromIndex(lastLogIndex + 1)
				action = append(action, Send{from: msg.FromId, event: AppendEntriesReq{Term: s.CurrentTerm, LeaderId: s.LeaderId, LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm, LogEntries: newLogEntries, LeaderCommitIndex: s.CommitIndex}})

			} else {
				//We got true success in reponse, update the MatchIndex
				if msg.LastLogIndex+int(msg.CountOfEntries)+1 > int(s.MatchIndex[id]) {
					// update matchIndex if necessary
					s.MatchIndex[id] = msg.LastLogIndex + int(msg.CountOfEntries)
				}
			}

			tempMatchIndex := make([]int, NO_OF_SERVERS)
			copy(tempMatchIndex, s.MatchIndex)
			sort.IntSlice(tempMatchIndex).Sort()
			N := tempMatchIndex[NO_OF_SERVERS/2]

			if N > s.CommitIndex && s.GetLogEntryAtIndex(N).Term == s.CurrentTerm {
				s.CommitIndex = N
				action = append(action, Commit{Index: s.CommitIndex, Data: s.GetLogEntryAtIndex(N).Data})
			}
		}
		if isTermUpdated {
			s.clearVoteMap()
			action = append(action, StateStore{CurrentTerm: s.CurrentTerm, VotedFor: s.VotedFor})
		}

	default:
		fmt.Printf("Event not recognised")
	}
	return action
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}