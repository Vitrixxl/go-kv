package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Value struct {
	Value string `bson:"value"`
	ExpAt int64  `bson:"exp_at"`
}

type Kv map[string]Value

const (
	SET_USAGE         = "USAGE : SET <key> <value> <exp?>"
	GET_USAGE         = "USAGE : GET <key>"
	SUBSCRIBE_USAGE   = "USAGE : SUBSCRIBE <key>"
	UNSUBSCRIBE_USAGE = "USAGE : UNSUBSCRIBE <key>"
)

// The key will be the key of the suscribed data
type SuscribedConn map[string]net.Conn
type Subscribtion struct {
	Key string
	// Used to find faster the corresponding subscribtion
	Addr string
	Conn net.Conn
}

type Event struct {
	Addr  string
	Key   string
	Value string
}

type AppData struct {
	Kv
	ConnMap   map[string]net.Conn
	EventChan chan *Event
}

func sendError(conn net.Conn, error string) {

}

func eventListener(appData *AppData) {
	for {
		event := <-appData.EventChan
		conn := appData.ConnMap[event.Addr]
		conn.Write([]byte(fmt.Sprintf("%s %s\n", event.Key, event.Value)))
	}
}

func expirationLoop(appData *AppData) {
	for {
		now := time.Now().Unix()
		for key, value := range appData.Kv {
			if now > value.ExpAt {
				delete(appData.Kv, key)
			}
		}
		time.Sleep(time.Second)
	}
}

// Exp will be set a -1 if the exp isn't specified
func handleSet(key string, value string, expAt int64, appData *AppData, connAddr string) {
	appData.Kv[key] = Value{Value: value, ExpAt: expAt}
	appData.EventChan <- &Event{
		Addr:  connAddr,
		Key:   key,
		Value: value,
	}
}

func handleGet(conn net.Conn, key string, appData *AppData) {
	_, err := conn.Write([]byte(fmt.Sprintf("%s %s\n", key, appData.Kv[key].Value)))
	if err != nil {
		log.Printf("Error while sending the value : %s\n", err.Error())
	}
}

func handleConnection(conn net.Conn, appData *AppData) {
	reader := bufio.NewReader(conn)
	connAddr := conn.RemoteAddr().String()

	for {

		byteMessage, _, err := reader.ReadLine()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Printf("Error while reading the line : %s\n", err.Error())
			continue
		}

		fmt.Println(string(byteMessage))
		args := strings.Split(strings.ToLower(string(byteMessage)), " ")
		argsLen := len(args)
		log.Printf("Query recieved : %s", strings.Join(args, " "))

		switch args[0] {
		case "set":
			if argsLen != 3 {
				sendError(conn, fmt.Sprintf("Invalid arguments, %s", SET_USAGE))
				continue
			}
			exp, err := strconv.ParseInt(args[2], 10, 32)
			if err != nil {
				log.Printf("Error while parsing exp : %s\n", err.Error())
				sendError(conn, fmt.Sprintf("Invalid date exp : %s", SET_USAGE))
				continue
			}
			handleSet(args[0], args[1], exp, appData, connAddr)
			break

		case "get":
			if argsLen != 1 {
				sendError(conn, fmt.Sprintf("Invalid arguments, %s", GET_USAGE))
				continue
			}
			handleGet(conn, args[0], appData)
			break
		default:
			break
		}
	}
}

func main() {
	ln, err := net.Listen("tcp", ":6969")

	if err != nil {
		os.Stderr.Write([]byte(fmt.Sprintf("Error while listening tcp : %s\n", err.Error())))
		os.Exit(1)
	}

	appData := &AppData{
		Kv:        make(Kv),
		ConnMap:   make(map[string]net.Conn),
		EventChan: make(chan *Event),
	}

	go eventListener(appData)
	go expirationLoop(appData)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error while accepting the connection : %s\n", err.Error())
			continue
		}
		handleConnection(conn, appData)
	}
}
