package main

import (
	"bytes"
	"errors"
	"fmt"
	filesystem "github.com/kedareab/cs733/assignment4/filesystem/fs"
	raft "github.com/kedareab/cs733/assignment4/raftnode"
	"github.com/kedareab/cs733/assignment4/utils"
	"strings"
	"sync"
	"testing"
	"time"
)

var clientHandlers []*ClientHandler

func TestInitialSetup(t *testing.T) {

	var config *raft.Config
	config, err := raft.ReadConfigFile(CONFIG_FILE_NAME)
	if err != nil {
		t.Fatal("Error in reading config file", CONFIG_FILE_NAME)
	}

	for i := 0; i < len(config.Clientports); i++ {
		clientHandlers = append(clientHandlers, New(i, config))
		clientHandlers[i].run()
	}
	time.Sleep(3 * time.Second)
}

func expect(t *testing.T, response *filesystem.Msg, expected *filesystem.Msg, errstr string, err error) {
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

func eTestRPC_BasicSequential(t *testing.T) {
	cl := utils.MakeClient("localhost:8003")
	defer cl.Close()

	// Read non-existent file cs733net
	m, err := cl.Read("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'F'}, "file not found1", err)

	// Delete non-existent file cs733net
	m, err = cl.Delete("cs733net2")
	expect(t, m, &filesystem.Msg{Kind: 'F'}, "file not found2", err)

	// Write file cs733net
	data := "Cloud fun"
	m, err = cl.Write("cs733net", data, 0)
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = cl.Read("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

	// CAS in new value
	version1 := m.Version
	data2 := "Cloud fun 2"
	// Cas new value
	m, err = cl.Cas("cs733net", version1, data2, 0)
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "cas success", err)

	// Expect to read it back
	m, err = cl.Read("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte(data2)}, "read my cas", err)

	// Expect Cas to fail with old version
	m, err = cl.Cas("cs733net", version1, data, 0)
	expect(t, m, &filesystem.Msg{Kind: 'V'}, "cas version mismatch", err)

	// Expect a failed cas to not have succeeded. Read should return data2.
	m, err = cl.Read("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte(data2)}, "failed cas to not have succeeded", err)

	// delete
	m, err = cl.Delete("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "delete success", err)

	// Expect to not find the file
	m, err = cl.Read("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'F'}, "file not found", err)
}

func TestRPC_Binary(t *testing.T) {
	cl := utils.MakeClient("localhost:8004")
	defer cl.Close()

	// Write binary contents
	data := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	m, err := cl.Write("binfile", data, 0)
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = cl.Read("binfile")
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

}

func eTestRPC_Chunks(t *testing.T) {
	// Should be able to accept a few bytes at a time
	cl := utils.MakeClient("localhost:8004")
	defer cl.Close()
	var err error
	snd := func(chunk string) {
		if err == nil {
			err = cl.Send(chunk)
		}
	}

	// Expect to not find the file
	m, err := cl.Read("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'F'}, "file not found", err)

	// Send the command "write teststream 10\r\nabcdefghij\r\n" in multiple chunks
	// Nagle's algorithm is disabled on a write, so the server should get these in separate TCP packets.
	snd("wr")
	time.Sleep(10 * time.Millisecond)
	snd("ite test")
	time.Sleep(10 * time.Millisecond)
	snd("stream 1")
	time.Sleep(10 * time.Millisecond)
	snd("0\r\nabcdefghij\r")
	time.Sleep(10 * time.Millisecond)
	snd("\n")

	m, err = cl.Rcv()
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "writing in chunks should work", err)
}

func eTestRPC_Batch(t *testing.T) {
	// Send multiple commands in one batch, expect multiple responses
	cl := utils.MakeClient("localhost:8004")
	defer cl.Close()

	// Expect to not find the file
	m, err := cl.Read("cs733net")
	expect(t, m, &filesystem.Msg{Kind: 'F'}, "file not found", err)

	cmds := "write batch1 3\r\nabc\r\n" +
		"write batch2 4\r\ndefg\r\n" +
		"read batch1\r\n"

	cl.Send(cmds)
	m, err = cl.Rcv()
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "write batch1 success", err)
	m, err = cl.Rcv()
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "write batch2 success", err)
	m, err = cl.Rcv()
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte("abc")}, "read batch1", err)
}

func eTestRPC_BasicTimer(t *testing.T) {
	cl := utils.MakeClient("localhost:8004")
	defer cl.Close()

	// Write file cs733, with expiry time of 2 seconds
	str := "Cloud fun"
	m, err := cl.Write("cs733", str, 2)
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back immediately.
	m, err = cl.Read("cs733")
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err)

	time.Sleep(3 * time.Second)

	// Expect to not find the file after expiry
	m, err = cl.Read("cs733")
	expect(t, m, &filesystem.Msg{Kind: 'F'}, "file not found", err)

	// Recreate the file with expiry time of 1 second
	m, err = cl.Write("cs733", str, 1)
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "file recreated", err)

	// Overwrite the file with expiry time of 4. This should be the new time.
	m, err = cl.Write("cs733", str, 3)
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "file overwriten with exptime=4", err)

	// The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
	time.Sleep(2 * time.Second)

	// Expect the file to not have expired.
	m, err = cl.Read("cs733")
	expect(t, m, &filesystem.Msg{Kind: 'C', Contents: []byte(str)}, "file to not expire until 4 sec", err)

	time.Sleep(3 * time.Second)
	// 5 seconds since the last write. Expect the file to have expired
	m, err = cl.Read("cs733")
	expect(t, m, &filesystem.Msg{Kind: 'F'}, "file not found after 4 sec", err)

	// Create the file with an expiry time of 1 sec. We're going to delete it
	// then immediately create it. The new file better not get deleted.
	m, err = cl.Write("cs733", str, 1)
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "file created for delete", err)

	m, err = cl.Delete("cs733")
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "deleted ok", err)

	m, err = cl.Write("cs733", str, 0) // No expiry
	expect(t, m, &filesystem.Msg{Kind: 'O'}, "file recreated", err)

	time.Sleep(1100 * time.Millisecond) // A little more than 1 sec
	m, err = cl.Read("cs733")
	expect(t, m, &filesystem.Msg{Kind: 'C'}, "file should not be deleted", err)
}

func eTestRPC_ConcurrentWrites(t *testing.T) {
	nclients := 2
	niters := 10
	clients := make([]*utils.Client, nclients)
	for i := 0; i < nclients; i++ {
		cl := utils.MakeClient("localhost:8001")
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.Close()
		clients[i] = cl
	}

	errCh := make(chan error, nclients)
	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
	sem.Add(1)
	ch := make(chan *filesystem.Msg, nclients*niters) // channel for all replies
	for i := 0; i < nclients; i++ {
		go func(i int, cl *utils.Client) {
			sem.Wait()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				m, err := cl.Write("concWrite", str, 0)
				if err != nil {
					errCh <- err
					break
				} else {
					ch <- m
				}
			}
		}(i, clients[i])
	}
	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Go!

	// There should be no errors
	for i := 0; i < nclients*niters; i++ {
		select {
		case m := <-ch:
			if m.Kind != 'O' {
				t.Fatalf("Concurrent write failed with kind=%c", m.Kind)
			}
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	m, _ := clients[0].Read("concWrite")
	// Ensure the contents are of the form "cl <i> 9"
	// The last write of any client ends with " 9"
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 9")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg = %v", m)
	}
}

func eTestRPC_MultipleWrites(t *testing.T) {
	nclients := 1
	niters := 100
	clients := make([]*utils.Client, nclients)
	for i := 0; i < nclients; i++ {
		cl := utils.MakeClient("localhost:8001")
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.Close()
		clients[i] = cl
	}

	errCh := make(chan error, nclients)
	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
	sem.Add(1)
	ch := make(chan *filesystem.Msg, nclients*niters) // channel for all replies
	for i := 0; i < nclients; i++ {
		go func(i int, cl *utils.Client) {
			sem.Wait()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				m, err := cl.Write("concWrite", str, 0)
				if err != nil {
					errCh <- err
					break
				} else {
					ch <- m
				}
			}
		}(i, clients[i])
	}
	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Go!

	// There should be no errors
	for i := 0; i < nclients*niters; i++ {
		select {
		case m := <-ch:
			if m.Kind != 'O' {
				t.Fatalf("Concurrent write failed with kind=%c", m.Kind)
			}
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	m, _ := clients[0].Read("concWrite")
	// Ensure the contents are of the form "cl <i> 9"
	// The last write of any client ends with " 9"
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), "9")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg = %v", m)
	}
}

func eTestRPC_ConcurrentCas(t *testing.T) {
	nclients := 2
	niters := 10

	clients := make([]*utils.Client, nclients)
	for i := 0; i < nclients; i++ {
		cl := utils.MakeClient("localhost:8001")
		if cl == nil {
			t.Fatalf("Unable to create client #%d", i)
		}
		defer cl.Close()
		clients[i] = cl
	}

	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to *begin* concurrently
	sem.Add(1)

	m, _ := clients[0].Write("concCas", "first", 0)
	ver := m.Version
	if m.Kind != 'O' || ver == 0 {
		t.Fatalf("Expected write to succeed and return version")
	}

	var wg sync.WaitGroup
	wg.Add(nclients)

	errorCh := make(chan error, nclients)

	for i := 0; i < nclients; i++ {
		go func(i int, ver int, cl *utils.Client) {
			sem.Wait()
			defer wg.Done()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				for {
					m, err := cl.Cas("concCas", ver, str, 0)
					if err != nil {
						errorCh <- err
						return
					} else if m.Kind == 'O' {
						break
					} else if m.Kind != 'V' {
						errorCh <- errors.New(fmt.Sprintf("Expected 'V' msg, got %c", m.Kind))
						return
					}
					ver = m.Version // retry with latest version
				}
			}
		}(i, ver, clients[i])
	}

	time.Sleep(100 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Start goroutines
	wg.Wait()                          // Wait for them to finish
	select {
	case e := <-errorCh:
		t.Fatalf("Error received while doing cas: %v", e)
	default: // no errors
	}
	m, _ = clients[0].Read("concCas")
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 9")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
	}
}

func TestRPCEnd(t *testing.T) {
	for i := 0; i < 5; i++ {
		// Start client handler
		clientHandlers[i].Shutdown()
	}
}
