package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/Saf1u/pubsubshared/pubsubtypes"
	"github.com/gin-gonic/gin"
)

type PubSub struct {
	publish      net.Listener
	registration net.Listener
	topics       map[string]*Topic
	server       *http.Server
}

type topicResponse struct {
	Topic       string `json:"topic"`
	Subscribers int    `json:"subscribers"`
}

type topicRequest struct {
	Topic string `json:"topicname"`
}

func BuildPubSub(publishAddr string, registerAddr string, topicNames []string) *PubSub {
	pub, err := net.Listen("tcp", publishAddr)
	if err != nil {
		panic(err)
	}
	sub, err := net.Listen("tcp", registerAddr)
	if err != nil {
		panic(err)
	}
	router := gin.Default()
	router.Use(CORSMiddleware())
	server := &http.Server{
		Addr:    ":8082",
		Handler: router,
	}
	pubsub := &PubSub{pub, sub, make(map[string]*Topic), server}

	for _, topicName := range topicNames {
		pubsub.topics[topicName] = &Topic{topicName, make(map[int]*subscriber), 0, 0, &sync.Mutex{}}
	}

	router.Handle("GET", "/topics", func(c *gin.Context) {
		topics := []topicResponse{}
		for _, topic := range pubsub.topics {

			topics = append(topics, topicResponse{topic.topic, topic.numberOfSubs})
		}
		c.JSON(200, topics)
	})

	router.Handle("POST", "/topics", func(c *gin.Context) {
		topicReq := &topicRequest{}
		data, err := ioutil.ReadAll(c.Request.Body)
		if err != nil {
			panic(err)
		}
		json.Unmarshal(data, topicReq)
		pubsub.topics[topicReq.Topic] = &Topic{topicReq.Topic, make(map[int]*subscriber), 0, 0, &sync.Mutex{}}
		c.JSON(200, nil)
	})
	log.Println("registered topics:", topicNames)
	go pubsub.server.ListenAndServe()
	return pubsub
}

const (
	CLOSE_CONN    = "close"
	REGISTER_CONN = "register"
)

func (p *PubSub) HandleRegistration() {
	for {
		conn, err := p.registration.Accept()
		if err != nil {
			panic(err)
		}
		buffer := make([]byte, 1024)
		conn.Read(buffer[0:1])
		conn.Read(buffer[1 : buffer[0]+1])
		reader := bytes.NewReader(buffer[1 : buffer[0]+1])
		decoder := gob.NewDecoder(reader)
		msg := &pubsubtypes.Message{}
		err = decoder.Decode(msg)
		if err != nil {
			panic(err)
		}
		if msg.Type == REGISTER_CONN {
			id := p.topics[msg.Topic].AddSuscriber(conn)
			regMsg := &pubsubtypes.Message{Id: id}
			p.topics[msg.Topic].subscribers[id].put(*regMsg)
		} else {
			p.topics[msg.Topic].RemoveSubscriber(msg.Id)
		}
	}
}
func (p *PubSub) AcceptMessages() {
	for {
		conn, err := p.publish.Accept()
		if err != nil {
			panic(err)
		}
		go func(conn net.Conn) {
			data := make([]byte, 1024)
			n, err := conn.Read(data)
			if err != nil {
				panic(err)
			}
			decoder := gob.NewDecoder(bytes.NewReader(data[0:n]))
			msg := &pubsubtypes.Message{}
			err = decoder.Decode(msg)
			if err != nil {
				panic(err)
			}
			p.topics[msg.Topic].Publish(*msg)
		}(conn)
	}
}

type Topic struct {
	topic          string
	subscribers    map[int]*subscriber
	nextIdentifier int
	numberOfSubs   int
	lock           *sync.Mutex
}

func (t *Topic) Publish(message pubsubtypes.Message) error {
	for _, subscriber := range t.subscribers {
		go subscriber.put(message)
	}
	return nil
}

func (t *Topic) AddSuscriber(conn net.Conn) int {
	t.lock.Lock()
	defer t.lock.Unlock()
	sus := &subscriber{t.nextIdentifier, make(chan pubsubtypes.Message, 500), bytes.NewBuffer(make([]byte, 0, 1024)), conn}
	t.subscribers[t.nextIdentifier] = sus
	log.Println("subscriber registered on topic: ", t.topic, " with id:", t.nextIdentifier)
	id := t.nextIdentifier
	t.nextIdentifier++
	t.numberOfSubs++
	go func(identifier int) {

		err := sus.read()
		if err != nil {
			t.RemoveSubscriber(identifier)
		}

	}(id)
	return id

}

func (t *Topic) RemoveSubscriber(id int) int {
	t.subscribers[id].put(pubsubtypes.Message{Type: CLOSE_CONN})
	t.numberOfSubs--
	delete(t.subscribers, id)
	log.Println("subscriber removed from topic: ", t.topic, " with id:", id)
	return id

}

type subscriber struct {
	Id      int
	message chan pubsubtypes.Message
	buffer  *bytes.Buffer
	conn    net.Conn
}

func (s *subscriber) put(message pubsubtypes.Message) error {
	if message.Type == CLOSE_CONN {
		close(s.message)
		return nil
	}
	s.message <- message
	return nil
}

func (s *subscriber) read() error {
	for {
		msg, stat := <-s.message
		if !stat {
			s.conn.Close()
			log.Println(s.Id, ":closed subscription on topic")
			return nil
		}
		s.buffer.Reset()
		encoder := gob.NewEncoder(s.buffer)
		err := encoder.Encode(msg)
		if err != nil {
			panic(err)
		}
		toSend := s.buffer.Bytes()
		length := make([]byte, 1)
		length[0] = byte(s.buffer.Len())
		length = append(length, toSend...)

		_, err = s.conn.Write(length)
		if err != nil {
			return err
		}
		//conn probably not explicitly closed if a write failed
	}

}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		c.Next()
	}
}
