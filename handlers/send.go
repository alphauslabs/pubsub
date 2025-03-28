package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/golang/glog"
)

const (
	topicsubupdates      = "topicsubupdates"
	checkleader          = "checkleader"
	initialTopicSubFetch = "initialtopicsubfetch"
	initialmsgsfetch     = "initialmsgsfetch"

	LockmsgEvent = "lockmsg"
)

type SendInput struct {
	Type string
	Msg  []byte
}

var ctrlsend = map[string]func(*app.PubSub, []byte) ([]byte, error){
	topicsubupdates:      handleTopicSubUpdates,
	checkleader:          handleCheckLeader,
	initialTopicSubFetch: handleInitializeTopicSub,
	initialmsgsfetch:     handleInitialMsgsFetch,
	LockmsgEvent:         handleLockMessage,
}

// Root handler for op.Send()
func Send(data any, msg []byte) ([]byte, error) {
	var in SendInput
	app := data.(*app.PubSub)

	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}

	return ctrlsend[in.Type](app, in.Msg)
}

// Handle topic subscription updates.
func handleTopicSubUpdates(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()

	FetchAndBroadcast(ctx, app, false) // trigger topic-subscription fetch and broadcast
	return nil, nil
}

func handleInitializeTopicSub(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()

	topicsub := FetchAllTopicSubscriptions(ctx, app.Client) // trigger topic-subscription fetch
	msgData, err := json.Marshal(topicsub)
	if err != nil {
		glog.Errorf("STRUCT-Error marshalling topicSub: %v", err)
		return nil, err
	}

	return msgData, nil
}

func handleInitialMsgsFetch(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()

	// Fetch all messages
	BroadcastAllMessages(ctx, app)

	return nil, nil
}

func handleLockMessage(app *app.PubSub, msg []byte) ([]byte, error) {
	m := string(msg)
	mms := strings.Split(m, ":")

	if len(mms) != 2 {
		glog.Errorf("[Lock] Invalid message format: %s", m)
		return nil, fmt.Errorf("invalid message format")
	}

	messageId := mms[0]
	sub := mms[1]

	// retrieve the message from storage
	message, err := storage.GetMessage(messageId)
	if err != nil {
		glog.Errorf("[Lock] Error retrieving message %s: %v", messageId, err)
		return nil, err
	}
	if message == nil {
		glog.Errorf("[Lock] Message %s not found", messageId)
		return nil, fmt.Errorf("message not found")
	}

	message.Mu.Lock()
	if message.Subscriptions[sub].IsDeleted() {
		glog.Errorf("[Lock] Message=%s already done/deleted for sub=%s", messageId, sub)
		return nil, fmt.Errorf("message already done/deleted")
	}

	if message.Subscriptions[sub].IsLocked() {
		glog.Errorf("[Lock] Message=%s already locked for sub=%s", messageId, sub)
		return nil, fmt.Errorf("message already locked")
	}
	message.Mu.Unlock()

	// Ask all nodes to lock this message
	broadcastData := BroadCastInput{
		Type: MsgEvent,
		Msg:  []byte(fmt.Sprintf("lock:%s:%s", messageId, sub)),
	}
	bin, _ := json.Marshal(broadcastData)
	out := app.Op.Broadcast(context.Background(), bin)
	for _, o := range out {
		if o.Error != nil {
			glog.Errorf("[Subscribe] Error broadcasting lock: %v", o.Error)
			return nil, o.Error
		}
	}

	glog.Infof("[Lock-leader] Message=%s locked successfully for sub=%s", messageId, sub)
	return nil, nil
}

func handleCheckLeader(app *app.PubSub, msg []byte) ([]byte, error) {
	if string(msg) == "PING" {
		return []byte("PONG"), nil
	}

	return nil, nil
}
