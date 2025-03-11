// helpers.go
package main

// Checks if the topic subscription is correct, and exists in the memory
// func (s *server) checkIfTopicSubscriptionIsCorrect(topicID, subscription string) error {
// 	glog.Infof("[Subscribe] Checking if subscription exists for topic: %s", topicID)
// 	subs, err := storage.GetSubscribtionsForTopic(topicID)

// 	if err != nil {
// 		glog.Infof("[Subscribe] Topic %s not found in storage", topicID)
// 		return status.Errorf(codes.NotFound, "Topic %s not found", topicID)
// 	}

// 	glog.Infof("[Subscribe] Found subscriptions for topic %s: %v", topicID, subs)

// 	// Check if the provided subscription ID exists in the topic's subscriptions
// 	found := false
// 	for _, sub := range subs {
// 		if sub.Name == subscription {
// 			found = true
// 			glog.Infof("[Subscribe] Subscription %s found in topic %s", subscription, topicID)
// 			break
// 		}
// 	}

// 	if !found {
// 		glog.Infof("[Subscribe] Subscription %s not found in topic %s", subscription, topicID)
// 		return status.Errorf(codes.NotFound, "Subscription %s not found", subscription)
// 	}

// 	return nil
// }

// // ExtendVisibilityTimeout extends the visibility timeout for a message
// func (s *server) ExtendVisibilityTimeout(messageID string, subscriberID string, visibilityTimeout time.Duration) error {
// 	value, exists := s.MessageLocks.Load(messageID)
// 	if !exists {
// 		return status.Error(codes.NotFound, "message not locked")
// 	}

// 	info, ok := value.(handlers.MessageLockInfo)
// 	if !ok {
// 		return status.Error(codes.Internal, "invalid lock info")
// 	}

// 	// Retrieve subscription to check AutoExtend
// 	sub, err := s.GetSubscription(context.TODO(), &pb.GetSubscriptionRequest{Name: subscriberID})
// 	if err != nil {
// 		return status.Error(codes.NotFound, "subscription not found")
// 	}

// 	// Log AutoExtend status
// 	glog.Infof("[ExtendVisibility] Subscription %s has AutoExtend: %v", sub.Name, sub.Autoextend)

// 	// If AutoExtend is disabled, allow manual extension
// 	if !sub.Autoextend {
// 		glog.Infof("[ExtendVisibility] Autoextend is disabled. Allowing manual extension.")
// 	}

// 	// Extend visibility timeout
// 	newExpiresAt := time.Now().Add(visibilityTimeout)
// 	info.Timeout = newExpiresAt
// 	s.MessageLocks.Store(messageID, info)

// 	// Broadcast new timeout
// 	broadcastData := handlers.BroadCastInput{
// 		Type: handlers.MsgEvent,
// 		Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", messageID, int(visibilityTimeout.Seconds()), s.Op.HostPort())),
// 	}
// 	msgBytes, _ := json.Marshal(broadcastData)

// 	s.Op.Broadcast(context.TODO(), msgBytes)
// 	glog.Infof("[ExtendVisibility] Visibility timeout extended for message: %s by subscriber: %s", messageID, subscriberID)

// 	return nil
// }

// AutoExtendTimeout automatically extends the visibility timeout if autoextend is enabled
// func (s *server) AutoExtendTimeout(messageID string, subscriberID string, visibilityTimeout time.Duration) {
// 	value, exists := s.MessageLocks.Load(messageID)
// 	if !exists {
// 		glog.Infof("[AutoExtend] Message %s not found or already processed", messageID)
// 		return
// 	}

// 	info, ok := value.(handlers.MessageLockInfo)
// 	if !ok {
// 		glog.Errorf("[AutoExtend] Invalid lock info for message %s", messageID)
// 		return
// 	}

// 	// Check if this node owns the lock
// 	if info.NodeID != s.Op.HostPort() {
// 		glog.Infof("[AutoExtend] Skipping extension for message %s (not owned by this node)", messageID)
// 		return
// 	}

// 	// Ensure only autoextend-enabled messages get extended

// 	sub, err := storage.GetSubscribtionsForTopic(subscriberID)
// 	if err != nil {
// 		glog.Errorf("[AutoExtend] Failed to fetch subscription %s: %v", subscriberID, err)
// 		return
// 	}

// 	if !sub[0].Autoextend {
// 		glog.Infof("[AutoExtend] Subscription %s does not have autoextend enabled", subscriberID)
// 		return
// 	}
// 	// sub, err := storage.GetSubscription(subscriberID) // todo: commented to fix errros, please uncomment if needed
// 	// if err != nil {
// 	// 	glog.Errorf("[AutoExtend] Failed to fetch subscription %s: %v", subscriberID, err)
// 	// 	return
// 	// }
// 	// if !sub.Autoextend {
// 	// 	glog.Infof("[AutoExtend] Subscription %s does not have autoextend enabled", subscriberID)
// 	// 	return
// 	//

// 	// Extend visibility timeout
// 	newExpiresAt := time.Now().Add(visibilityTimeout)
// 	info.Timeout = newExpiresAt
// 	s.MessageLocks.Store(messageID, info)

// 	// Broadcast new timeout
// 	broadcastData := handlers.BroadCastInput{
// 		Type: handlers.MsgEvent,
// 		Msg:  []byte(fmt.Sprintf("autoextend:%s:%d:%s", messageID, int(visibilityTimeout.Seconds()), s.Op.HostPort())),
// 	}
// 	msgBytes, _ := json.Marshal(broadcastData)

// 	// Send broadcast to all nodes
// 	s.Op.Broadcast(context.TODO(), msgBytes)
// 	glog.Infof("[AutoExtend] Node %s auto-extended timeout for message: %s", s.Op.HostPort(), messageID)
// }

// HandleBroadcastMessage processes broadcast messages received from other nodes
// func (s *server) HandleBroadcastMessage(msgType string, msgData []byte) error {
// 	// This method would be called by your broadcast handler
// 	switch msgType {
// 	case "lock":
// 		parts := strings.Split(string(msgData), ":")
// 		if len(parts) < 3 {
// 			return fmt.Errorf("invalid lock message format")
// 		}
// 		messageID := parts[0]
// 		timeoutSecondsStr := parts[1]
// 		subscriberID := parts[2]

// 		timeoutSeconds, err := strconv.Atoi(timeoutSecondsStr)
// 		if err != nil {
// 			return err
// 		}

// 		// Store the lock locally
// 		lockInfo := handlers.MessageLockInfo{
// 			Timeout:      time.Now().Add(time.Duration(timeoutSeconds) * time.Second),
// 			Locked:       true,
// 			NodeID:       s.Op.HostPort(), // This is the current node
// 			SubscriberID: subscriberID,
// 			LockHolders:  make(map[string]bool),
// 		}
// 		s.MessageLocks.Store(messageID, lockInfo)

// 	case "unlock":
// 		messageID := string(msgData)
// 		s.MessageLocks.Delete(messageID)

// 	}

// 	return nil
// }
