package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Node struct {
	ID       string
	Nodes    []string
	Leader   bool
	Term     uint
	VotedFor string
	Log      []LogEntry
	Lock     sync.Mutex
	//Status   string
}

type LogEntry struct {
	Term    uint   `json:"term"`
	Command string `json:"command"`
}

type RequestVoteRequest struct {
	Term         uint `json:"term"`
	CandidateID  uint `json:"candidateId"`
	LastLogIndex int  `json:"lastLogIndex"`
	LastLogTerm  uint `json:"lastLogTerm"`
}

type RequestVoteResponse struct {
	Term        uint `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type AppendEntriesRequest struct {
	Term         uint       `json:"term"`
	LeaderID     uint       `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  uint       `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit uint       `json:"leaderCommit"`
}

type AppendEntriesResponse struct {
	Term    uint `json:"term"`
	Success bool `json:"success"`
}

type CommandRequest struct {
	Command string `json:"command"`
}

type CommandResponse struct {
	Result string `json:"result"`
}

func (n *Node) RequestVoteHandler(w http.ResponseWriter, r *http.Request) {
	var req RequestVoteRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := RequestVoteResponse{
		Term:        n.Term,
		VoteGranted: false,
	}

	n.Lock.Lock()
	defer n.Lock.Unlock()

	if req.Term > n.Term {
		n.Term = req.Term
		n.VotedFor = ""
		n.Leader = false
		resp.Term = n.Term
		if req.LastLogTerm > n.Log[len(n.Log)-1].Term || (req.LastLogTerm == n.Log[len(n.Log)-1].Term && req.LastLogIndex >= len(n.Log)-1) {
			n.VotedFor = strconv.Itoa(int(req.CandidateID))
			resp.VoteGranted = true
		}

	}

	json.NewEncoder(w).Encode(resp)
}

func (n *Node) AppendEntriesHandler(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := AppendEntriesResponse{
		Term:    n.Term,
		Success: false,
	}

	n.Lock.Lock()
	defer n.Lock.Unlock()

	if req.Term >= n.Term {
		n.Term = req.Term
		n.VotedFor = ""
		n.Leader = false
		resp.Term = n.Term

		if len(n.Log)-1 < req.PrevLogIndex || n.Log[req.PrevLogIndex].Term != req.PrevLogTerm {
			resp.Success = false
			return
		}

		n.Log = n.Log[:req.PrevLogIndex+1]
		n.Log = append(n.Log, req.Entries...)

		if req.LeaderCommit > n.Log[len(n.Log)-1].Term {
			n.Log = n.Log[:len(n.Log)-1]
			n.Log = append(n.Log, LogEntry{Term: req.LeaderCommit})
		}

		n.Leader = true
		resp.Success = true
	}

	json.NewEncoder(w).Encode(resp)
}
func (n *Node) GetStatusHandler(w http.ResponseWriter, r *http.Request) {
	status := "Follower"
	if n.Leader {
		status = "Leader"
	}
	fmt.Fprintf(w, "Node %s is currently %s", n.ID, status)
}

func (n *Node) CommandHandler(w http.ResponseWriter, r *http.Request) {
	var req CommandRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := CommandResponse{
		Result: "",
	}

	if !n.Leader {
		resp.Result = "Not leader"
		json.NewEncoder(w).Encode(resp)
		return
	}

	entry := LogEntry{
		Term:    n.Term,
		Command: req.Command,
	}

	n.Lock.Lock()
	defer n.Lock.Unlock()

	n.Log = append(n.Log, entry)
	resp.Result = "Success"

	json.NewEncoder(w).Encode(resp)
}
func (n *Node) Start() {
	rand.Seed(time.Now().UnixNano())
	port := flag.Int("port", 0, "Port to listen on")
	flag.Parse()
	if *port == 0 {
		log.Fatal("Please specify a port to listen on with -port")
	}

	n.ID = strconv.Itoa(rand.Int())
	http.HandleFunc("/request_vote", n.RequestVoteHandler)
	http.HandleFunc("/append_entries", n.AppendEntriesHandler)
	http.HandleFunc("/command", n.CommandHandler)
	http.HandleFunc("/status", n.GetStatusHandler)

	log.Printf("Starting node %v on port %v", n.ID, *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", *port), nil))
}

// func main() {

// 	node := Node{}
// 	node.Start()
// }
