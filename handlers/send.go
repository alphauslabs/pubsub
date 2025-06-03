package handlers

import (
	"context"
	"encoding/json"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	topicsubupdates      = "topicsubupdates"
	checkleader          = "checkleader"
	initialTopicSubFetch = "initialtopicsubfetch"
	allmessages          = "allmessages"
	requestRecordMap     = "requestrecordmap"
)

type SendInput struct {
	Type string
	Msg  []byte
}

var ctrlsend = map[string]func(*app.PubSub, []byte) ([]byte, error){
	topicsubupdates:      handleTopicSubUpdates,
	checkleader:          handleCheckLeader,
	initialTopicSubFetch: handleInitializeTopicSub,
	allmessages:          handleAllMessages,
	requestRecordMap:     handleRequestRecordMap,
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
	me := string(msg)
	members := storage.GetMembersFromRecordMap(storage.RecordMap, me)
	if members == nil {
		return nil, status.Errorf(codes.Internal, "no members found in record map")
	}
	grouped := utils.CreateGrouping(topicsub, members)
	msgData, err := json.Marshal(grouped)
	if err != nil {
		glog.Errorf("STRUCT-Error marshalling topicSub: %v", err)
		return nil, err
	}

	glog.Infof("[InitializeTopicSub] grouped: %v", string(msgData))

	return msgData, nil
}

func handleAllMessages(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()

	// Fetch all messages
	BroadcastAllMessages(ctx, app)

	return nil, nil
}

// func handleLockMessage(app *app.PubSub, msg []byte) ([]byte, error) {
// 	m := string(msg)
// 	mms := strings.Split(m, ":")

// 	if len(mms) != 2 {
// 		glog.Errorf("[Handlelock] Invalid message format: %s", m)
// 		return nil, fmt.Errorf("invalid message format")
// 	}

// 	messageId := mms[0]
// 	sub := mms[1]

// 	// Retrieve the message from storage
// 	message, err := storage.GetMessage(messageId)
// 	if err != nil {
// 		glog.Errorf("[Handlelock] Error retrieving message %s: %v", messageId, err)
// 		return nil, err
// 	}

// 	message.Mu.RLock()
// 	if message.Subscriptions[sub].IsDeleted() {
// 		glog.Errorf("[Handlelock} Message=%s already done/deleted for sub=%s", messageId, sub)
// 		return nil, fmt.Errorf("message already done/deleted")
// 	}

// 	if message.Subscriptions[sub].IsLocked() {
// 		glog.Errorf("[Handlelock] Message=%s already locked for sub=%s", messageId, sub)
// 		return nil, fmt.Errorf("message already locked")
// 	}
// 	message.Mu.RUnlock()

// 	// Ask all nodes to lock this message
// 	broadcastData := BroadCastInput{
// 		Type: MsgEvent,
// 		Msg:  []byte(fmt.Sprintf("lock:%s:%s", messageId, sub)),
// 	}
// 	bin, _ := json.Marshal(broadcastData)
// 	out := app.Op.Broadcast(context.Background(), bin)
// 	for _, o := range out {
// 		if o.Error != nil {
// 			glog.Errorf("[Subscribe] Error broadcasting lock: %v", o.Error)
// 			return nil, o.Error
// 		}
// 	}

// 	glog.Infof("[Lock-leader] Message=%s locked successfully for sub=%s", messageId, sub)
// 	return nil, nil
// }

func handleCheckLeader(app *app.PubSub, msg []byte) ([]byte, error) {
	if string(msg) == "PING" {
		return []byte("PONG"), nil
	}

	return nil, nil
}

func handleRequestRecordMap(app *app.PubSub, msg []byte) ([]byte, error) {
	b, err := json.Marshal(storage.RecordMap)
	if err != nil {
		glog.Errorf("Error marshalling record map: %v", err)
		return nil, err
	}

	return b, nil
}
