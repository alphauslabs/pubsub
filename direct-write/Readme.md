TASK:

Test VM-driven, isolated writes direct to Spanner

direct write - like normal http server that will accept message from our test publisher then write to spanner


endpointURL = "http://<VM_IP>:8080/write"

Key Features

✅ Initializes a single Spanner client for the entire server (instead of creating a new one per request).

✅ Accepts JSON payloads via a POST /write API endpoint.

✅ Writes data to Spanner using InsertOrUpdate, which:

Inserts a new record if the id doesn’t exist.

Updates an existing record if the id already exists.

✅ Logs errors for debugging.


IN TERMINAL 1:

run the test3.go

![image](https://github.com/user-attachments/assets/3403d39f-f7ed-41da-9958-26252fdb4e35)



IN TERMINAL2 - To test it locally:

curl -X POST http://localhost:8080/write      -H "Content-Type: application/json"      -d '{
           "id": "12345",
           "subsription": "test_subscription",
           "payload": "Hello from VM"
         }'

![image](https://github.com/user-attachments/assets/db8fdf42-0e70-4f02-8691-77e4cde03cf1)


What Happens After Sending?

-The server receives the request

-It parses the JSON

-It writes the data to Spanner

-It responds with success or failure


