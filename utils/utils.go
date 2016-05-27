package utils

import (
	"strconv"
	"fmt"
	"net"
	"bufio"
	"strings"
	"errors"
	filesystem "github.com/kedareab/cs733/assignment4/filesystem/fs"
)

//----------------------------------------------------------------------
// Utility functions


func (cl *Client) Read(filename string) (*filesystem.Msg, error) {
	cmd := "read " + filename + "\r\n"
	return cl.SendRcv(cmd)
}

func (cl *Client) Write(filename string, contents string, exptime int) (*filesystem.Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
	} else {
		cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.SendRcv(cmd)
}

func (cl *Client) Cas(filename string, version int, contents string, exptime int) (*filesystem.Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
	} else {
		cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.SendRcv(cmd)
}

func (cl *Client) Delete(filename string) (*filesystem.Msg, error) {
	cmd := "delete " + filename + "\r\n"
	return cl.SendRcv(cmd)
}

var errNoConn = errors.New("Connection is closed")

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}

func MakeClient(serverURL string) *Client {
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", serverURL)
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
		} else {
			fmt.Errorf(err.Error())
		}
	}
	if err != nil {
		fmt.Errorf(err.Error())
	}

	return client
}

func (cl *Client) Send(str string) error {
	if cl.conn == nil {
		return errNoConn
	}
	_, err := cl.conn.Write([]byte(str))
	if err != nil {
		err = fmt.Errorf("Write error in SendRaw: %v", err)
		cl.conn.Close()
		cl.conn = nil
	}
	return err
}

func (cl * Client)SendRcv(str string) (msg *filesystem.Msg, err error) {
	if cl.conn == nil {
		return nil, errNoConn
	}
	err = cl.Send(str)
	if err == nil {
		msg, err = cl.Rcv()
	}

	//If msg.Kind is Redirect Change the client connection and retry
	if msg != nil && msg.Kind == 'R' {

		changeClientConnection(cl, msg.RedirectURL)
		msg, err = cl.SendRcv(str)
	} else if msg != nil && msg.Kind == 'I' {
		msg, err = cl.SendRcv(str)
	}

	return msg, err
}

//Closes the current client connection and assigns to new serverURL
func changeClientConnection (cl *Client, serverURL string) {
	cl.Close()
	newClient := MakeClient(serverURL)
	cl.conn = newClient.conn
	cl.reader = newClient.reader
}

func (cl *Client) Close() {
	if cl != nil && cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}

func (cl *Client) Rcv() (msg *filesystem.Msg, err error) {
	// we will assume no errors in server side formatting
	line, err := cl.reader.ReadString('\n')
	if err == nil {
		msg, err = parseFirst(line)
		if err != nil {
			return nil, err
		}
		if msg.Kind == 'C' {
			contents := make([]byte, msg.Numbytes)
			var c byte
			for i := 0; i < msg.Numbytes; i++ {
				if c, err = cl.reader.ReadByte(); err != nil {
					break
				}
				contents[i] = c
			}
			if err == nil {
				msg.Contents = contents
				cl.reader.ReadByte() // \r
				cl.reader.ReadByte() // \n
			}
		}
	}
	if err != nil {
		fmt.Printf("\n Error in rcv() at Utils:143  %v", err)
		cl.Close()
	}
	return msg, err
}

func parseFirst(line string) (msg *filesystem.Msg, err error) {
	fields := strings.Fields(line)
	msg = &filesystem.Msg{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int {
		var i int
		if err == nil {
			if fieldNum >=  len(fields) {
				err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
				return 0
			}
			i, err = strconv.Atoi(fields[fieldNum])
		}
		return i
	}

	if len(fields) == 0 {
		return nil, errors.New("Empty line. The previous command is likely at fault")
	}
	switch fields[0] {
	case "OK": // OK [version]
		msg.Kind = 'O'
		if len(fields) > 1 {
			msg.Version = toInt(1)
		}
	case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
		msg.Kind = 'C'
		msg.Version = toInt(1)
		msg.Numbytes = toInt(2)
		msg.Exptime = toInt(3)
	case "ERR_VERSION":
		msg.Kind = 'V'
		msg.Version = toInt(1)
	case "ERR_FILE_NOT_FOUND":
		msg.Kind = 'F'
	case "ERR_CMD_ERR":
		msg.Kind = 'M'
	case "ERR_INTERNAL":
		msg.Kind = 'I'
	case "ERR_REDIRECT":
		msg.Kind = 'R'
		msg.RedirectURL = fields[1]
	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		fmt.Printf("\n Error in ParseFirst at Utils:195  %v", err)
		return nil, err
	} else {
		return msg, nil
	}
}
