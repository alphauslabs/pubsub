package main

// func handler(w http.ResponseWriter, r *http.Request) {
// 	if r.Method == http.MethodPost {
// 		body, _ := ioutil.ReadAll(r.Body)
// 		glog.Infof("Server 8080 received: %s", body)
// 		fmt.Fprintf(w, "Received at server 8080")
// 	} else {
// 		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
// 	}
// }

// func main() {
// 	http.HandleFunc("/", handler)
// 	glog.Info("Starting server on port 8080")
// 	log.Fatal(http.ListenAndServe(":8080", nil))
// }
