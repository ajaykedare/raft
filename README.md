# A Replicated Filesystem Using Raft


# Description
This is an implementation of file server which provides four basic functionalities as read, write, delete and cas, and  is replicated using the raft consensus algorithm.

Raft Consensus Algorithm :

[Raft: In Search of an Understandable Consensus Algorithm](https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf) gives the overall implementation of raft consesnsus algorithm and has explained how raft is easy to understand and  implement than Paxos.

The basic idea to develope this has taken from paper disucssed above with some modification in designing.



# Sample Usage

1. Start a file system

    ```
    > cd $GOPATH/src/github.com/kedareab/raft
    > go build
    > ./raft 100 &
    > ./raft 200 &
    > ./raft 300 &
    > ./raft 400 &
    > ./raft 500 &
     ```

2. Start a making requests to file server

* A user friendly client program as
     ```
    > cd $GOPATH/src/github.com/kedareab/raft/client
    > go build
    > ./client localhost 8001
    ```

* Or you can spawn a separate telnet client to talk with the file server as below:

     ```
     > telnet localhost 8001
       Connected to localhost.
       Escape character is '^]'
       read hello
       ERR_FILE_NOT_FOUND
       write hello 6
       abcdef
       OK 1
       read hello
       CONTENTS 1 6 0
       abcdef
       cas hello 1 7 0
       ghijklm
       OK 2
       read hello
       CONTENTS 2 7 0
       ghijklm
     ```

#Installation
```
    go get github.com/kedareab/raft
    go test github.com/kedareab/raft
```

# Command Specification and valid responses
```
1. Write:
       write <filename> <numbytes> [<exptime>]\r\n
       <content bytes>\r\n
    Write response:
       OK <version>
2. Read:
       read <filename>\r\n
    Read response:
       CONTENTS <version> <numbytes> <exptime> \r\n
       <content bytes>\r\n
3. CAS: (Compare and Swap)
       cas <filename> <version> <numbytes> [<exptime>]\r\n
       <content bytes>\r\n
    Cas response:
       OK <version>\r\n
4. Delete:
       delete <filename>\r\n
     Delete response:
       OK\r\n
5. Possible errors from these commands (instead of OK)
     ERR_VERSION\r\n
     ERR_FILE_NOT_FOUND\r\n
     ERR_CMD_ERR\r\n
     ERR_INTERNAL\r\n
     ERR_REDIRECT <url>\r\n
```

## Config file structure
Make changes in port number as in when required

```
{
  "Id":100,
  "LogDir":"Log",
  "PersistenceStateDir":"raftnode/PersistenceStateFiles",
  "ElectionTimeout":3000,
  "HeartbeatTimeout":500,
  "InboxSize": 5000,
  "OutboxSize":5000,
  "Clientports":[8001,8002,8003,8004,8006],
  "Cluster":[
    {
      "Id": 100,
      "Host": "localhost",
      "Port": 9001
    }
  ]
}
```


`This work has used the log,cluster packages from github.com/cs733-iitb/ by Prof. Sriram Srinivasan`

# Contact
Ajay Kedare `153059007`

ajaykedare007 at gmail.com

MTech in Computer Science and Engineering

`IIT Bombay`
