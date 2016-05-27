package main

import (
	"fmt"
	"os"
	"strconv"
	"bufio"
	"strings"
	filesystem "github.com/kedareab/cs733/assignment4/filesystem/fs"
	utils "github.com/kedareab/cs733/assignment4/utils"
)

var crlf = []byte{'\r', '\n'}

func usage () {
	fmt.Println("Usage : [read|write|cas|delete]")
	fmt.Println("      : read   <filename>")
	fmt.Println("      : write  <filename> <content>")
	fmt.Println("      : cas    <filename> <version> <content>")
	fmt.Println("      : delete <filename>")
}

func usageRun () {
	fmt.Println("Please tun as :")
	fmt.Println("      : go run client.go utils.go  <hostaddress> <port>")
}

// Provide message reply to the conn object
func writeToConsole( msg *filesystem.Msg) bool {
	var err error
	write := func(data []byte) {
		fmt.Printf(string(data))
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

func main() {
	expectArgs := func (n int) {
		if len(os.Args) < n {
			usageRun()
			os.Exit(1)
		}
	}
	expectArgs(3)
	var msg *filesystem.Msg
	var err error
	port,_:=strconv.Atoi(os.Args[2])
	host := os.Args[1]
	cl := utils.MakeClient(fmt.Sprintf("%s:%d",host,port))

	if cl == nil {
		fmt.Println("Unable to connect to raft servers")
		os.Exit(2)
	}

	var input string
	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("Enter Command: ")
		input, _ = reader.ReadString('\n')
		fields := strings.Fields(input)
		switch fields[0] {
			case "read" :
				if len(fields) !=2 {
					fmt.Printf("Please check the command format again as below!\n")
					usage()
				} else {
					filename := fields[1]
					msg, err = cl.Read(filename)
					if err == nil {
						writeToConsole(msg)
					} else {
						fmt.Printf("Msg : %+v\nErr : %v\n", msg, err)
					}
				}
			case "write" :
				if len(fields) !=3 {
					fmt.Printf("Please check the command format again as below!\n")
					usage()
				} else {
					filename := fields[1]
					msg, err = cl.Write(filename, fields[2], 0)
					if err == nil {
						writeToConsole(msg)
					} else {
						fmt.Printf("Msg : %+v\nErr : %v\n", msg, err)
					}
				}
			case "cas":
				if len(fields) !=4 {
					fmt.Printf("Please check the command format again as below!\n")
					usage()
				} else {
					filename := fields[1]
					ver,_:= strconv.Atoi(fields[2])
					msg, err = cl.Cas(filename, ver, fields[3], 0)
					if err == nil {
						writeToConsole(msg)
					} else {
						fmt.Printf("Msg : %+v\nErr : %v\n", msg, err)
					}
				}
			case "delete":
				if len(fields) !=2 {
					fmt.Printf("Please check the command format again as below!\n")
					usage()
				} else {
					filename := fields[1]
					msg, err = cl.Delete(filename)
					if err == nil {
						writeToConsole(msg)
					} else {
						fmt.Printf("Msg : %+v\nErr : %v\n", msg, err)
					}
				}
			case "exit":
				fmt.Printf("\n Exiting the program!")
			default:
				fmt.Println("Invalid Command")
				usage()
		}

		if fields[0] == "exit" {
			os.Exit(1)
		}

	}
}