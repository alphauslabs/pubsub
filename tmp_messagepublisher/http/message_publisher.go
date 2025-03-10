package main

// import (
// 	"bytes"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"net/http"
// 	"sync"
// 	"time"

// 	"github.com/golang/glog"
// )

// type Message struct {
// 	ID      string `json:"id"`
// 	Topic   string `json:"topic"`
// 	Payload string `json:"payload"`
// }

// var (
// 	numMessages  = flag.Int("numMessages", 10000, "Number of messages to publish")
// 	useMock      = flag.Bool("mock", false, "Use localhost mock servers (ports 8080, 8081, 8082)")
// 	useBulkWrite = flag.Bool("bulkwrite", false, "Use bulk write GCP endpoints (ports 8080)")
// )

// var (
// 	directWriteEndpoints = []string{
// 		"http://10.146.0.43:8085/write",
// 		"http://10.146.0.46:8085/write",
// 		"http://10.146.0.51:8085/write",
// 	}

// 	bulkWriteEndpoints = []string{
// 		"http://10.146.0.43:8080/write",
// 		"http://10.146.0.46:8080/write",
// 		"http://10.146.0.51:8080/write",
// 	}

// 	mockEndpoints = []string{
// 		"http://localhost:8080/write",
// 		"http://localhost:8081/write",
// 		"http://localhost:8082/write",
// 	}
// 	activeEndpoints []string
// )

// func publishMessage(wg *sync.WaitGroup, id int, endpoint string) {
// 	defer wg.Done()

// 	topicID := fmt.Sprintf("topic-%d", id%10000)

// 	msg := Message{
// 		ID:      fmt.Sprintf("%d", id),
// 		Topic:   topicID,
// 		Payload: fmt.Sprintf("MESSAGE TO NODE %d", id%len(activeEndpoints)+1),
// 	}

// 	data, err := json.Marshal(msg)
// 	if err != nil {
// 		glog.Infof("%s Message %d failed: %v", time.Now().Format(time.RFC3339), id, err)
// 		return
// 	}

// 	_, err = http.Post(endpoint, "application/json", bytes.NewBuffer(data))
// 	if err != nil {
// 		glog.Infof("%s Message %d (ID: %s) failed: %v", time.Now().Format(time.RFC3339), id, msg.ID, err)
// 		return
// 	}

// 	glog.Infof("%s Message %d (ID: %s) published successfully", time.Now().Format(time.RFC3339), id, msg.ID)
// }

// func main() {
// 	flag.Parse()

// 	if *useMock {
// 		activeEndpoints = mockEndpoints
// 		glog.Info("Using mock servers at localhost:8080, localhost:8081, localhost:8082")
// 	} else if *useBulkWrite {
// 		activeEndpoints = bulkWriteEndpoints
// 		glog.Info("Using bulk write GCP endpoints (ports 8080)")
// 	} else {
// 		activeEndpoints = directWriteEndpoints
// 		glog.Info("Using direct write GCP endpoints (ports 8085)")
// 	}

// 	var wg sync.WaitGroup
// 	startTime := time.Now()

// 	for i := 0; i < *numMessages; i++ {
// 		wg.Add(1)
// 		endpoint := activeEndpoints[i%len(activeEndpoints)] // Round-robin distribution
// 		go publishMessage(&wg, i, endpoint)
// 	}

// 	wg.Wait()
// 	glog.Infof("All messages published. Total: %d, Time: %.2f s, Message per second: %.2f", *numMessages, time.Since(startTime).Seconds(), float64(*numMessages)/time.Since(startTime).Seconds())
// }
