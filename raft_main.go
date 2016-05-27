package main

import (
	"fmt"
	raft "github.com/kedareab/cs733/assignment4/raftnode"
	"log"
	"os"
	"os/exec"
	"strconv"
)

func main() {

	var config *raft.Config
	config, err := raft.ReadConfigFile(CONFIG_FILE_NAME)
	if err != nil {
		fmt.Println("Error in reading config file", CONFIG_FILE_NAME)
	}

	if len(os.Args) == 1 {
		cmds := make([]*exec.Cmd, 5)
		for i := 0; i < 5; i++ {
			cmds[i] = exec.Command(os.Args[0], strconv.Itoa(i))
			err := cmds[i].Start()
			if err != nil {
				log.Println(err)
			}
		}
		for _, cmd := range cmds {
			err := cmd.Wait()
			if err != nil {
				log.Println(err)
			}
		}

	} else {
		index, _ := strconv.Atoi(os.Args[1])
		clientHandler := New(index, config)
		clientHandler.Start()
	}
}
