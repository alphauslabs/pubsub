package main

// func handler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method == http.MethodPost {
// 		body, _ := ioutil.ReadAll(r.Body)
// 		log.Printf("Server 8081 received: %s", body)
// 		fmt.Fprintf(w, "Received at server 8081")
// 	} else {
// 		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
// 	}
// }

// func main() {
// 	http.HandleFunc("/", handler)
// 	log.Println("Starting server on port 8081")
// 	log.Fatal(http.ListenAndServe(":8081", nil))
// }
