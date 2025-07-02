package sdk

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

type KvClientConfig struct {
	defautExp int64
}

type SetParams struct {
	Key   string
	Value string
	Exp   int64
}

type KvClient struct {
	conn       net.Conn
	expDefault int64
	// Map with the key set by the user and the function attached to this
	// Which will be triggered whenever a change happen
	subscriberMap map[string](func(string) any)
}

func (kvClient *KvClient) Get(key string) (string, error) {
	_, err := kvClient.conn.Write([]byte("GET " + key + "\n"))
	if err != nil {
		return "", err
	}
	value, err := kvClient.listendResponse(key)
	return value, err
}

func (kvClient *KvClient) listendResponse(key string) (string, error) {

	reader := bufio.NewReader(kvClient.conn)
	for {
		rawLine, _, err := reader.ReadLine()
		if err != nil {
			return "", err
		}
		line := string(rawLine)
		if !strings.HasPrefix(line, key) {
			continue
		}
		return strings.Split(line, " ")[1], nil
	}
}

func (kvClient *KvClient) Set(params SetParams) error {
	var actualExp int64
	if params.Exp == 0 {
		actualExp = params.Exp
	}

	_, err := kvClient.conn.Write([]byte(fmt.Sprintf("SET %s %s %d\n", params.Key, params.Value, actualExp)))

	if err != nil {
		return err
	}
	return nil
}

func (kvClient *KvClient) Subscribe(key string, fn func(string) any) {
	kvClient.subscriberMap[key] = fn
}

func (kvClient *KvClient) Unsubscribe(key string) {
	delete(kvClient.subscriberMap, key)
}

func (kvClient *KvClient) subscribtionLoop() {
	reader := bufio.NewReader(kvClient.conn)
	for {
		rawLine, _, err := reader.ReadLine()
		if err != nil {
		}
		line := string(rawLine)
		args := strings.Split(line, " ")
		if len(args) > 2 {
			log.Printf("[ERROR]: Invalid args recieved")
		}
		fn, ok := kvClient.subscriberMap[args[0]]
		if !ok {
			return
		}
		fn(args[1])
	}
}

func CreateKvClient(expDefault int64) (*KvClient, error) {
	conn, err := net.Dial("tcp", "localhost:6969")
	if err != nil {
		return nil, err
	}

	client := &KvClient{
		conn: conn,
	}

	go client.subscribtionLoop()
	return client, nil
}
