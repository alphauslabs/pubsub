the test1.go runs on local host and that is what i have tested initially and was successful on adding info to spanner using this curl:
curl -X POST "http://localhost:8080/insert" -d '{
  "id": "123",
  "subscription": "sub-1",
  "payload": "test payload"
}' -H "Content-Type: application/json"

the main.go is for the vm and did not test it yet 
