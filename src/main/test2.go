package main

import (
	"bytes"
	"encoding/json"
	"fmt"

	// "go/printer"
	"io"
	"log"
	"math"

	// "math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"
	// "context"
)

// Define types for Raft nodes and log entries
type node struct {
	ID          int
	State       string
	Term        int
	VotedFor    int
	Log         []logEntry
	CommitIndex int
	LastApplied int
	mutex       sync.Mutex
	LeaderID    int
	NextIndex   map[int]int
	MatchIndex  map[int]int
	self        int
	peers       []int
	PeerIDs     map[int]bool
	timer       time.Duration // added timer field
	isLeader    bool          // added isLeader field
	stopChan    chan bool
}

// resetTimer sets the timer to 0 when the node transitions from candidate to leader
func (n *node) resetTimer() {
	if n.State != "leader" {
		n.timer = 0
	}
}

type logEntry struct {
	Term    int
	Command string
}

// Define types for Raft message requests and responses
type voteRequest struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type voteResponse struct {
	Term        int
	VoteGranted bool
}

// Define global variables for node IDs and ports
var nodeIDs = []int{1, 2, 3, 4, 5}
var nodePorts = map[int]int{
	1: 8081,
	2: 8082,
	3: 8083,
	4: 8084,
	5: 8085,
}

type requestVoteResponse struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

type appendEntriesRequest struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []logEntry
	LeaderCommit int
}

type appendEntriesResponse struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}
type requestVoteRequest struct { //from one to other nodes (sendrequest)
	Term         int `json:"term"`
	CandidateID  int `json:"candidateId"`
	LastLogIndex int `json:"lastLogIndex"`
	LastLogTerm  int `json:"lastLogTerm"`
}

type commandRequest struct {
	Command string `json:"command"`
}

type commandResponse struct {
	Result string `json:"result"`
}

// getLastLogIndexAndTerm returns the index and term of the last log entry in the node's log.
func (n *node) getLastLogIndexAndTerm() (int, int) {
	if len(n.Log) == 0 {
		return 0, 0
	}
	lastIndex := len(n.Log) - 1
	return lastIndex, n.Log[lastIndex].Term
}

func (voter *node) requestVoteHandler(w http.ResponseWriter, r *http.Request) {
	var req requestVoteRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := requestVoteResponse{
		Term:        voter.Term,
		VoteGranted: false,
	}

	if req.Term < voter.Term {
		// Reply false if term < currentTerm (§5.1)
		json.NewEncoder(w).Encode(resp)
		return
	}

	if voter.VotedFor == -1 || voter.VotedFor == req.CandidateID {
		// If votedFor is null or candidateId, and candidate’s log is at
		// least as up-to-date as receiver’s log, grant vote
		lastLogIndex, lastLogTerm := voter.getLastLogIndexAndTerm()
		if req.LastLogTerm > lastLogTerm || (req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex) {
			voter.VotedFor = req.CandidateID
			resp.Term = voter.Term
			resp.VoteGranted = true
		}
	}

	json.NewEncoder(w).Encode(resp)
}

func (leader *node) appendEntriesHandler(w http.ResponseWriter, r *http.Request) {
	var req appendEntriesRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	resp := appendEntriesResponse{
		Term:    leader.Term,
		Success: false,
	}
	if req.Term < leader.Term {
		json.NewEncoder(w).Encode(resp)
		return
	}
	lastIndex, _ := leader.getLastLogIndexAndTerm()
	if req.PrevLogIndex > lastIndex || (req.PrevLogIndex >= 0 && leader.Log[req.PrevLogIndex].Term != req.PrevLogTerm) {
		json.NewEncoder(w).Encode(resp)
		return
	}
	for i, entry := range req.Entries {
		index := req.PrevLogIndex + i + 1
		if index >= len(leader.Log) {
			leader.Log = append(leader.Log, entry)
		} else if leader.Log[index].Term != entry.Term {
			leader.Log = leader.Log[:index]
			leader.Log = append(leader.Log, entry)
		}
	}
	if req.LeaderCommit > leader.CommitIndex {
		lastIndex, _ := leader.getLastLogIndexAndTerm()
		leader.CommitIndex = int(math.Min(float64(req.LeaderCommit), float64(lastIndex)))
	}

	resp.Term = leader.Term
	resp.Success = true
	json.NewEncoder(w).Encode(resp)
}

func (n *node) executeCommandHandler(w http.ResponseWriter, r *http.Request) {
	var req commandRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if n.isLeader {
		// If the node is the leader, append the command to its log
		n.mutex.Lock()
		defer n.mutex.Unlock()
		n.Log = append(n.Log, logEntry{
			Term:    n.Term,
			Command: req.Command,
		})
	} else {
		// If the node is not the leader, forward the command to the leader
		if n.LeaderID == -1 {
			// If the node doesn't know who the leader is, return an error
			http.Error(w, "No leader", http.StatusInternalServerError)
			return
		}

		// Send the command to the leader
		url := fmt.Sprintf("http://localhost:%d/execute", nodePorts[n.LeaderID])
		jsonValue, _ := json.Marshal(req)
		resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonValue))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			http.Error(w, "Error forwarding command to leader", http.StatusInternalServerError)
			return
		}

		// Return the result from the leader
		var leaderResp commandResponse
		err = json.NewDecoder(resp.Body).Decode(&leaderResp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(leaderResp)
		return
	}

	// If the node is the leader, replicate the command to the followers
	for id := range nodeIDs {
		if id != n.ID {
			// Send the command to the follower
			url := fmt.Sprintf("http://localhost:%d/appendEntries", nodePorts[id])
			jsonValue, _ := json.Marshal(appendEntriesRequest{
				Term:         n.Term,
				LeaderID:     n.ID,
				PrevLogIndex: len(n.Log) - 2,
				PrevLogTerm:  n.Log[len(n.Log)-2].Term,
				Entries:      []logEntry{n.Log[len(n.Log)-1]},
				LeaderCommit: n.CommitIndex,
			})
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonValue))
			if err != nil {
				log.Printf("Error sending command to node %d: %v\n", id, err)
				continue
			}
			defer resp.Body.Close()

			var appendResp appendEntriesResponse
			err = json.NewDecoder(resp.Body).Decode(&appendResp)
			if err != nil {
				log.Printf("Error decoding response from node %d: %v\n", id, err)
				continue
			}

			if appendResp.Term > n.Term {
				// If the follower's term is greater than the leader's term, step down as leader
				n.Term = appendResp.Term
				n.VotedFor = -1
				n.State = "follower"
				n.isLeader = false
				n.LeaderID = -1
				log.Printf("Node %d stepped down as leader because of higher term from node %d\n", n.ID, id)
				return
			}
		}
	}
	json.NewEncoder(w).Encode(commandResponse{"Command executed successfully"})
}

// func ElectionTimer() time.Duration {
// 	// Generate a random seed based on the current time
// 	rand.Seed(time.Now().UnixNano())

// 	// Generate a random number between 0 and 15 (inclusive)
// 	randomNumber := rand.Intn(16)

// 	// Add 15 seconds to the random number to get a value in the range of 15 to 30 seconds
// 	randomDuration := time.Duration(randomNumber+15) * time.Second

//		return randomDuration
//	}
func sendHeartBeatToNode(port string, wg *sync.WaitGroup) {
	defer wg.Done()
	url := fmt.Sprintf("http://localhost:%s/heartbeat", port)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println("Error sending heartbeat to node on port", port, ":", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Error sending heartbeat to node on port", port, ", status code:", resp.StatusCode)
		return
	}

	fmt.Println("Heartbeat sent to node on port", port)
}

func heartBeat() {
	wg := sync.WaitGroup{}
	wg.Add(len(nodePorts))
	ticker := time.NewTicker(time.Minute * 1)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, port := range nodePorts {
				go sendHeartBeatToNode(strconv.Itoa(port), &wg)
			}
		}
	}
}
func (n *node) GetStatusHandler(w http.ResponseWriter, r *http.Request) {
	//status := "Follower"
	fmt.Fprintf(w, "Node %d is currently %s", n.ID, n.State)
}

// Implement sendVoteRequest function
func (n *node) sendVoteRequestToNodes(vr voteRequest) {

	requestBodyBytes, err := json.Marshal(vr)
	if err != nil {
		fmt.Println("Error serializing VoteRequest:", err)
		return
	}
	var voteCount int = 0
	var maxCount int = len(nodePorts)/2 + 1
	// Iterate over the list of node ports
	for _, port := range nodePorts {
		client := http.Client{
			Timeout: 15 * time.Second, // Set a timeout for the request
		}
		// Construct the URL for the VoteRequest
		url := fmt.Sprintf("http://localhost:%d/requestVote", port)

		// Create a new HTTP POST request with the serialized JSON body
		req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBodyBytes))
		if err != nil {
			fmt.Println("Error creating HTTP request:", err)
			continue
		}

		// Set the Content-Type header to indicate that the request body is JSON
		req.Header.Set("Content-Type", "application/json")

		// Send the HTTP request

		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("Error sending HTTP request:", err)
			continue
		}

		// Check the response status code and handle accordingly
		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Received non-OK response from %s: %s\n", url, resp.Status)
			continue
		}

		// Read the response body
		var vr voteResponse
		err = json.NewDecoder(resp.Body).Decode(&vr)
		if err != nil {
			fmt.Println("Error decoding response body:", err)
			continue
		}
		if vr.VoteGranted {
			voteCount++
		}
		if voteCount >= maxCount {
			n.isLeader = true
			n.State = "leader"
			print("This node is leader %d", n.ID)
			n.resetTimer()
			heartBeat()
		}
		resp.Body.Close()
	}
}

func sendAppendEntriesRequest(n *node, url string, req appendEntriesRequest) {
	// Send the request to the target node
	resp, err := sendAppendEntriesRPC(url, req)
	if err != nil {
		log.Printf("Failed to send appendEntries request to %s: %v\n", url, err)
		return
	}
	// Handle the response from the target node
	var aeResp appendEntriesResponse
	err = json.Unmarshal(resp, &aeResp)
	if err != nil {
		log.Printf("Failed to parse appendEntries response from %s: %v\n", url, err)
		return
	}

	if aeResp.Term > n.Term {
		n.Term = aeResp.Term
		n.LeaderID = 0
		n.State = "follower"
	} else if aeResp.Success {
		n.State = "leader"
	}
}

func sendAppendEntriesRPC(url string, req appendEntriesRequest) ([]byte, error) {
	jsonReq, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(jsonReq))
	if err != nil {
		return nil, fmt.Errorf("failed to send HTTP request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("received non-200 response: %s", resp.Status)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %v", err)
	}

	return respBody, nil
}
func Start(k string) {
	var nodeID, err = strconv.Atoi(k)
	if err != nil {
		return
	}
	node := &node{
		ID:          nodeID,
		State:       "follower",
		Term:        0,
		VotedFor:    -1,
		Log:         []logEntry{},
		CommitIndex: -1,
		LastApplied: -1,
		mutex:       sync.Mutex{},
		LeaderID:    -1,
		NextIndex:   make(map[int]int),
		MatchIndex:  make(map[int]int),
		timer:       0,
		isLeader:    false,
	}

	// Set up HTTP handlers for the node
	http.HandleFunc("/requestVote", node.requestVoteHandler)
	http.HandleFunc("/appendEntries", node.appendEntriesHandler)
	http.HandleFunc("/status", node.GetStatusHandler)
	http.HandleFunc("/executeCommand", func(w http.ResponseWriter, r *http.Request) {
		node.executeCommandHandler(w, r)
	})

	// Start the HTTP server for the node
	port := nodePorts[nodeID]
	server := &http.Server{Addr: ":" + strconv.Itoa(port)}
	go func() {
		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	// Shutdown the HTTP server
}
func (n *node) startElections() {
	print("Started Elections")
	n.State = "candidate"
	n.Term++
	n.VotedFor = n.ID
	n.isLeader = false
	n.LeaderID = -1
	n.resetTimer()

	lastLogIndex, lastLogTerm := n.getLastLogIndexAndTerm()
	args := voteRequest{
		Term:         n.Term,
		CandidateID:  n.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Send vote requests to all other nodes
	// var voteChans []chan voteResponse
	var wg sync.WaitGroup
	for _, id := range nodeIDs {
		wg.Add(1)
		defer wg.Done()
		if id != n.ID {
			// voteChan := make(chan voteResponse)
			// voteChans = append(voteChans, voteChan)
			//
			go func() {
				requestBodyBytes, err := json.Marshal(args)
				if err != nil {
					fmt.Println("Error serializing VoteRequest:", err)
					return
				}
				// var voteCount int = 0
				// var maxCount int = len(nodePorts)/2 + 1
				client := http.Client{
					Timeout: 15 * time.Second, // Set a timeout for the request
				}
				// Construct the URL for the VoteRequest
				url := fmt.Sprintf("http://localhost:800%d/requestVote", id)

				// Create a new HTTP POST request with the serialized JSON body
				req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestBodyBytes))
				if err != nil {
					fmt.Println("Error creating HTTP request:", err)
				}

				// Set the Content-Type header to indicate that the request body is JSON
				req.Header.Set("Content-Type", "application/json")

				// Send the HTTP request

				resp, err := client.Do(req)
				if err != nil {
					fmt.Println("Error sending HTTP request:", err)
				}
				fmt.Println("res is ", resp)
				// Check the response status code and handle accordingly
				// if resp.StatusCode != http.StatusOK {
				// 	fmt.Printf("Received non-OK response from %s: %s\n", url, resp.Status)
				// }

				// // Read the response body
				// var vr voteResponse
				// err = json.NewDecoder(resp.Body).Decode(&vr)
				// if err != nil {
				// 	fmt.Println("Error decoding response body:", err)
				// }
				// if vr.VoteGranted {
				// 	voteCount++
				// }
				// if voteCount >= maxCount {
				// 	n.isLeader = true
				// 	n.State = "leader"
				// 	print("This node is leader %d", n.ID)
				// 	n.resetTimer()
				// 	heartBeat()
				// }
				// resp.Body.Close()
			}()

		}
	}
	wg.Wait()

	// votesReceived := 1 // vote for self
	// votesNeeded := len(nodeIDs) / 2

	// for {
	// 	select {
	// 	case voteResp := <-voteChans[0]:
	// 		if voteResp.Term > n.Term {
	// 			n.State = "follower"
	// 			return
	// 		}
	// 		if voteResp.VoteGranted {
	// 			votesReceived++
	// 		}
	// 		if votesReceived > votesNeeded {
	// 			n.isLeader = true
	// 			n.State = "leader"
	// 			heartBeat()
	// 			n.resetTimer()
	// 			return
	// 		}
	// 	case voteResp := <-voteChans[1]:
	// 		if voteResp.Term > n.Term {
	// 			n.State = "follower"
	// 			return
	// 		}
	// 		if voteResp.VoteGranted {
	// 			votesReceived++
	// 		}
	// 		if votesReceived > votesNeeded {
	// 			n.isLeader = true
	// 			n.State = "leader"
	// 			heartBeat()
	// 			n.resetTimer()
	// 			return
	// 		}
	// 	// continue for each channel in voteChans
	// 	case <-time.After(time.Duration(rand.Intn(200)+300) * time.Second):
	// 		n.startElections()
	// 		return
	// 	case <-n.stopChan:
	// 		return
	// 	}
	// }
}
func (n *node) initialize(self int, peers []int) {
	n.self = self
	n.peers = peers
	n.PeerIDs = make(map[int]bool)
	for _, peer := range peers {
		n.PeerIDs[peer] = true
	}
}
func main() {
	stopChan := make(chan os.Signal)
	signal.Notify(stopChan, os.Interrupt)
	var temp, err = strconv.Atoi(os.Args[1])
	if err != nil {
		return
	}
	go Start(os.Args[1])

	myMap := make(map[int]bool)
	var exclude = temp
	for i := 0; i < 5; i++ {
		if i == exclude {
			myMap[i] = false
		}
		if i != exclude {
			myMap[i] = true
		}
	}
	n := &node{
		ID:      temp,
		PeerIDs: myMap,
	}

	// initialize the node's properties
	n.initialize(temp, []int{1, 2, 3, 4, 5})

	// start the elections
	n.startElections()
}
