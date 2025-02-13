//ALL PLACEHOLDERS ATM - NON FUNCTIONAL

syntax = "proto3";

package pubsub;

service PubSubService {
  // Publish a message to a specific topic.
  rpc Publish(PublishRequest) returns (PublishResponse);
}

message PublishRequest {
  string topic = 1;
  string payload = 2;
  // TODO: OTHER METADATA FILES
}

message PublishResponse {
  string message_id = 1;
}
