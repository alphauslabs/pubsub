package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"flag"
)

type Message struct {
    ID           string `json:"id"`
    Subsription  string `json:"subsription"`
    Payload      string `json:"payload"`
}


var (
	numMessages=flag.Int("numMessages", 10000, "Number of messages to publish")
	useMock     = flag.Bool("mock", false, "Use localhost mock servers (ports 8080, 8081, 8082)")
	useBulkWrite = flag.Bool("bulkwrite", false, "Use bulk write GCP endpoints (ports 8080)")
)


var (
	directWriteEndpoints = []string{ 
		"http://10.146.0.4:8085/write",
		"http://10.146.0.8:8085/write",
		"http://10.146.0.18:8085/write",
	}
	
	 bulkWriteEndpoints = []string{
		"http://10.146.0.4:8080/write",
		"http://10.146.0.8:8080/write",
		"http://10.146.0.18:8080/write",
	}
	
	 mockEndpoints = []string{
		"http://localhost:8080/write",
		"http://localhost:8081/write",
		"http://localhost:8082/write",
	}
	activeEndpoints []string
)

func publishMessage(wg *sync.WaitGroup, id int, endpoint string) {
	defer wg.Done()
	msg := Message{
		ID:           fmt.Sprintf("%d", id),
		Subsription:  fmt.Sprintf("sub-%d", id),
		Payload:      fmt.Sprintf("MESSAGE TO NODE %d", id%len(activeEndpoints)+1),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("%s Message %d failed: %v", time.Now().Format(time.RFC3339), id, err)
		return
	}

	_, err = http.Post(endpoint, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("%s Message %d (ID: %s) failed: %v", time.Now().Format(time.RFC3339), id, msg.ID, err)
		return
	}

	log.Printf("%s Message %d (ID: %s) published successfully", time.Now().Format(time.RFC3339), id, msg.ID)
}


func main() {
	flag.Parse()

	if *useMock {
		activeEndpoints = mockEndpoints
		log.Println("Using mock servers at localhost:8080, localhost:8081, localhost:8082")
	} else if *useBulkWrite {
		activeEndpoints = bulkWriteEndpoints
		log.Println("Using bulk write GCP endpoints (ports 8080)")
	} else {
		activeEndpoints = directWriteEndpoints
		log.Println("Using direct write GCP endpoints (ports 8085)")
	}

	
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < *numMessages; i++ {
		wg.Add(1)
		endpoint := activeEndpoints[i%len(activeEndpoints)] // Round-robin distribution
		go publishMessage(&wg, i, endpoint)
	}

	wg.Wait()
	log.Printf("All messages published. Total: %d, Time: %.2f s, Message per second: %.2f", *numMessages, time.Since(startTime).Seconds(), float64(*numMessages)/time.Since(startTime).Seconds())
}
