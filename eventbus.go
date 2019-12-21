package eventbus

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

const DefaultKeepAliveTimeout = time.Second * 30

// An EventHandler responds to an event.
// If the Handle call returns an error, then the offset will not be recorded as
// processed.
type EventHandler interface {
	Handle(Message) error
}

// EventHandlerFunc is an adapter type to allow the use of ordinary functions
// as an EventHandler
type EventHandlerFunc func(Message) error

// Handle implements EventHandler for the EventHandlerFunc adapter type.
func (e EventHandlerFunc) Handle(m Message) error {
	return e(m)
}

// An Eventbus is the client for connecting to eventbus-sub.
type Eventbus struct {
	config           Config
	state            eventbusState
	socket           socketClient
	eventHandler     EventHandler
	dialer           dialer
	store            offsetStore
	Reconnection     ReconnectionScheduler
	startingOffset   int64
	KeepAliveTimeout time.Duration
	errorLogger      func(e error)
}

func (eb *Eventbus) sendBytes(data []byte) error {
	return eb.socket.WriteMessage(websocket.TextMessage, data)
}

func (eb *Eventbus) setState(s eventbusState) {
	eb.state = s
}

// StartAtNewest sets the offset to request from the most recent offsets, rather
// than from the start of the events recorded in the stream.
func (eb *Eventbus) StartAtNewest() {
	eb.startingOffset = OffsetNewest
}

func (eb *Eventbus) connect() error {
	eb.state = connecting{}
	reconnectTimeout, exit := eb.Reconnection.NextReconnectBackoff()
	if exit != nil {
		return exit
	}
	time.Sleep(reconnectTimeout)
	c, _, err := eb.dialer.Dial(eb.config.Endpoint, nil)
	if err != nil {
		return err
	}
	c.SetReadDeadline(time.Now().Add(eb.KeepAliveTimeout))
	pingHandler := c.PingHandler()
	c.SetPingHandler(func(s string) error {
		c.SetReadDeadline(time.Now().Add(eb.KeepAliveTimeout))
		pingHandler(s)
		return nil
	})
	eb.socket = c
	return nil
}

// Run starts the eventbus loop.
// When Run is called, the registered EventHandler will be called for each
// message in the stream.
// It returns a chan that the caller can wait on to receive errors during event
// streaming.
func (eb *Eventbus) Run() chan error {
	done := make(chan error)

	go func() {
		defer close(done)
		defer func() {
			if x := recover(); x != nil {
				err, ok := x.(error)
				if !ok {
					err = fmt.Errorf("%q", err)
				}
				done <- err
			}
			if eb.socket != nil {
				eb.socket.Close()
			}
		}()
		for {
			if eb.socket == nil {
				err := eb.connect()
				if err != nil {
					done <- err
					return
				}
			}
			_, msg, err := eb.socket.ReadMessage()
			if err != nil {
				eb.errorLogger(err)
				eb.socket.Close()
				eb.socket = nil
				continue
			}
			err = eb.state.handleEvent(eb, msg)
			if err != nil {
				eb.errorLogger(err)
				eb.socket.Close()
				eb.socket = nil
				continue
			}
		}
	}()
	return done
}

// SetErrorLogger allows configuration of the error logging mechanism.
func (eb *Eventbus) SetErrorLogger(el func(e error)) {
	eb.errorLogger = el
}

// TODO: this should probably verify that the fields are present.
func (eb Eventbus) createHandshake(serverID string) map[string]string {
	handshake := map[string]string{
		"id":             serverID,
		"authentication": eb.config.AuthToken,
		"stream":         eb.config.Stream,
		"client":         eb.config.Client,
		"version":        eb.config.Version,
	}
	offsets, err := eb.store.GetOffsets()
	if err == nil {
		if offsets == nil {
			handshake["state"] = encodeStarting(eb.startingOffset)
		} else {
			handshake["state"] = encodeOffsets(*offsets)
		}
	}
	return handshake
}

// NewEventbus creates a new Eventbus client to handle events.
func NewEventbus(config Config, handler EventHandler, store offsetStore) *Eventbus {
	return &Eventbus{
		config:           config,
		eventHandler:     handler,
		store:            store,
		dialer:           websocket.DefaultDialer,
		startingOffset:   OffsetOldest,
		Reconnection:     DefaultPolicy.NewScheduler(),
		KeepAliveTimeout: DefaultKeepAliveTimeout,
		errorLogger: func(err error) {
			log.Print(err.Error())
		},
	}
}

func encodeOffsets(offsets PartitionOffsets) string {
	data := map[string]PartitionOffsets{"p": offsets}
	encoded, err := json.Marshal(data)
	if err != nil {
		log.Panicf("unable to marshall partition offset data: %s", err)
	}
	return base64.StdEncoding.EncodeToString([]byte(encoded))
}

// Eventbus has an undocumented {"d": sarama offset} feature
func encodeStarting(position int64) string {
	data := map[string]string{"d": strconv.FormatInt(position, 10)}
	encoded, err := json.Marshal(data)
	if err != nil {
		log.Panicf("unable to marshall partition offset data: %s", err)
	}
	return base64.StdEncoding.EncodeToString([]byte(encoded))
}

// Config records the fields that are use to identify the eventbus client to the
// eventbus-sub service.
type Config struct {
	Endpoint  string
	AuthToken string
	Stream    string
	Client    string
	Version   string
}

type messageWriter interface {
	WriteMessage(int, []byte) error
}

type messageReader interface {
	ReadMessage() (int, []byte, error)
}

type messageCloser interface {
	Close() error
}

type socketClient interface {
	messageWriter
	messageReader
	messageCloser

	SetPingHandler(h func(appData string) error)
}

type dialer interface {
	Dial(string, http.Header) (*websocket.Conn, *http.Response, error)
}
