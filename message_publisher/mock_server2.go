package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func handler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		body, _ := ioutil.ReadAll(r.Body)
		log.Printf("Server 8082 received: %s", body)
		fmt.Fprintf(w, "Received at server 8082")
	} else {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func main() {
	http.HandleFunc("/", handler)
	log.Println("Starting server on port 8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
