package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"slices"
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

type AppData struct {
	Kv
	Subscribtions     []Subscribtion
	SubscribtionsChan chan string
}

func sendError(conn net.Conn, error string) {

}

func subscribtionLoop(appData *AppData) {
	for {
		key := <-appData.SubscribtionsChan
		for _, sub := range appData.Subscribtions {
			if sub.Key == key {
				sub.Conn.Write([]byte(key + " " + appData.Kv[key].Value))
			}
		}
	}
}

func unsubscribe(key string, appData *AppData) {
	for i, sub := range appData.Subscribtions {
		if sub.Key == key {
			appData.Subscribtions = slices.Delete(appData.Subscribtions, i, i)
		}
	}
}

func expirationLoop(appData *AppData) {
	for {
		now := time.Now().Unix()
		for key, value := range appData.Kv {
			if now > value.ExpAt {
				delete(appData.Kv, key)
				unsubscribe(key, appData)
			}
		}
		time.Sleep(time.Second)
	}
}

// Exp will be set a -1 if the exp isn't specified
func handleSet(key string, value string, expAt int64, appData *AppData) {
	appData.Kv[key] = Value{Value: value, ExpAt: expAt}
	appData.SubscribtionsChan <- key
}

func handleGet(conn net.Conn, key string, appData *AppData) {
	_, err := conn.Write([]byte(key + " " + appData.Kv[key].Value))
	if err != nil {
		log.Printf("Error while sending the value : %s\n", err.Error())
	}
}

func handleSubscribe(conn net.Conn, key string, appData *AppData) {
	appData.Subscribtions = append(appData.Subscribtions, Subscribtion{
		Key:  key,
		Addr: conn.RemoteAddr().String(),
		Conn: conn,
	})
}

func handleUnsubscribe(conn net.Conn, key string, appData *AppData) {
	connAddr := conn.RemoteAddr().String()
	for i, sub := range appData.Subscribtions {
		if sub.Addr == connAddr && sub.Key == key {
			appData.Subscribtions = append(appData.Subscribtions[i:], appData.Subscribtions[:i+1]...)
			break
		}
	}
}

func handleConnection(conn net.Conn, appData *AppData) {
	reader := bufio.NewReader(conn)
	for {

		byteMessage, _, err := reader.ReadLine()
		if err != nil {
			log.Printf("Error while reading the line : %s\n", err.Error())
			continue
		}
		args := strings.Split(strings.ToLower(string(byteMessage)), " ")
		argsLen := len(args)
		switch args[0] {
		case "set":
			if argsLen != 4 {
				sendError(conn, fmt.Sprintf("Invalid arguments, %s", SET_USAGE))
				continue
			}
			exp, err := strconv.ParseInt(args[3], 10, 32)
			if err != nil {
				log.Printf("Error while parsing exp : %s\n", err.Error())
				sendError(conn, fmt.Sprintf("Invalid date exp : %s", SET_USAGE))
				continue
			}
			handleSet(args[1], args[2], exp, appData)
			break
		case "get":
			if argsLen != 2 {
				sendError(conn, fmt.Sprintf("Invalid arguments, %s", GET_USAGE))
				continue
			}
			handleGet(conn, args[1], appData)

			break
		case "susbscribe":
			if argsLen != 2 {
				sendError(conn, fmt.Sprintf("Invalid arguments, %s", SUBSCRIBE_USAGE))
				continue
			}
			handleSubscribe(conn, args[1], appData)
			break
		case "unsubscribe":
			if argsLen != 2 {
				sendError(conn, fmt.Sprintf("Invalid arguments, %s", UNSUBSCRIBE_USAGE))
				continue
			}
			handleUnsubscribe(conn, args[1], appData)
		default:
			// a := make([]string, 4)
			//
			// sendError(conn, fmt.Sprintf("Invalid args, %s", strings.Join(make([]{SET_USAGE, GET_USAGE, SUBSCRIBE_USAGE, UNSUBSCRIBE_USAGE}), "\n")))

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
		Kv:                make(Kv),
		Subscribtions:     make([]Subscribtion, 0),
		SubscribtionsChan: make(chan string),
	}

	subscribtionLoop(appData)
	expirationLoop(appData)
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error while accepting the connection : %s\n", err.Error())
			continue
		}
		handleConnection(conn, appData)
	}
}
