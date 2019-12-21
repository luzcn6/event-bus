package eventbus

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type eventbusState interface {
	handleEvent(*Eventbus, []byte) error
}

type serverHandshake struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

type connecting struct{}

func (s connecting) handleEvent(eventbus *Eventbus, body []byte) error {
	var sh serverHandshake
	err := json.Unmarshal(body, &sh)
	if err != nil {
		return errors.Wrap(err, "unmarshalling body in connecting.handleEvent")
	}

	handshake := eventbus.createHandshake(sh.ID)
	response, err := json.Marshal(handshake)
	if err != nil {
		return errors.Wrap(err, "marshalling response in connecting.handleEvent")
	}

	err = eventbus.sendBytes(response)
	if err != nil {
		return errors.Wrap(err, "sending handshake in connecting.handleEvent")
	}
	eventbus.setState(ready{})
	return nil
}

type streamingEvent struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Stream string `json:"stream"`
}

type ready struct{}

func (s ready) handleEvent(eventbus *Eventbus, body []byte) error {
	var sm streamingEvent
	err := json.Unmarshal(body, &sm)
	if err != nil {
		return errors.Wrap(err, "unmarshalling body in ready.handleEvent")
	}
	eventbus.setState(streaming{})
	return nil
}

type streaming struct{}

type Message struct {
	Offset    int64           `json:"offset"`
	Partition int32           `json:"partition"`
	Body      json.RawMessage `json:"body"`
}

func (s streaming) handleEvent(eventbus *Eventbus, body []byte) error {
	var m Message
	err := json.Unmarshal(body, &m)
	if err != nil {
		return errors.Wrap(err, "unmarshalling body in streaming.handleEvent")
	}
	err = eventbus.eventHandler.Handle(m)
	if err != nil {
		return errors.Wrap(err, "handling event in streaming.handleEvent")
	}
	err = eventbus.store.SetOffset(m.Partition, m.Offset)
	if err != nil {
		return errors.Wrap(err, "storing offset in streaming.handleEvent")
	}
	return nil
}
