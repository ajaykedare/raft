package main

import (
	"testing"
	"os/exec"
	"github.com/kedareab/cs733/assignment4/utils"
	raft "github.com/kedareab/cs733/assignment4/raftnode"
	filesystem "github.com/kedareab/cs733/assignment4/filesystem/fs"
	"strconv"
	"fmt"
	"bytes"
	"log"
	"syscall"
	"time"
)


var raftnodeCommands []*exec.Cmd


func eTestInitialProcessSetup(t *testing.T) {

	var config *raft.Config
	config, err := raft.ReadConfigFile("config.json")
	if err != nil {
		t.Fatal("Error in reading config file : config.json" )
	}


	raftnodeCommands = make([]*exec.Cmd,len(config.Cluster))
	for i := 0; i < 5; i++ {
		// os.Args[0] works reliably to re-use the currently running executable
		// whether you built and ran it or used `go run`
		raftnodeCommands[i] = exec.Command("./assignment4", strconv.Itoa(i))
		err := raftnodeCommands[i].Start()
		if err != nil {
			log.Println(err)
		}
	}
	time.Sleep(3 * time.Second)
	/*for _, cmd := range raftnodeCommands {
		err := cmd.Wait()
		if err != nil {
			log.Println(err)
		}
	}*/
}

func eTestShutdownNode(t *testing.T) {

	cl := utils.MakeClient("localhost:8002")
	if cl==nil {
		t.Fatal("Client unable to connect.")
	}
	defer cl.Close()

	// Write file cs733
	str := "Cloud fun"
	m, err := cl.Write("cs733", str, 0)
	expectmain(t, m, &filesystem.Msg{Kind: 'O'}, "write success", err)


	//Shutdown one node
	raftnodeCommands[2].Process.Signal(syscall.SIGHUP)

	// Expect to read it back
	m, err = cl.Read("cs733")
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err)



}

func eTestShutdown (t *testing.T) {
	for i:=0; i< len(raftnodeCommands)  ; i++  {
		raftnodeCommands[i].Process.Signal(syscall.SIGHUP)
	}
}

func expectmain(t *testing.T, response *filesystem.Msg, expected *filesystem.Msg, errstr string, err error) {
	if err != nil {
		t.Fatal("Unexpected error: "+err.Error(), errstr)
	}
	ok := true
	if response.Kind != expected.Kind {
		ok = false
		errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Kind, expected.Kind)
	}
	if expected.Version > 0 && expected.Version != response.Version {
		ok = false
		errstr += " Version mismatch"
	}
	if response.Kind == 'C' {
		if expected.Contents != nil &&
		bytes.Compare(response.Contents, expected.Contents) != 0 {
			ok = false
		}
	}
	if !ok {
		t.Fatal("Expected " + errstr)
	}
}
