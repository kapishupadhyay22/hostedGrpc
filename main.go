package main

import (
	"log"
	"net"
	"os"
	"time"

	// This import path now matches your go.mod file
	"hehe/stream"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection" // Required for grpcurl to work without extra flags
	"google.golang.org/protobuf/types/known/structpb"
)

// server implements the StreamerServer interface defined in your proto file.
type server struct {
	// Embedding this provides default "unimplemented" responses,
	// which is a gRPC best practice for forward compatibility.
	stream.UnimplementedStreamerServer
}

// Subscribe is our implementation of the streaming RPC.
// This function overrides the default "unimplemented" behavior.
func (s *server) Subscribe(req *stream.StreamRequest, srv stream.Streamer_SubscribeServer) error {
	log.Printf("Received subscription request from client ID: %s", req.GetClientId())

	// We'll send a few different events every 2 seconds to simulate a real stream.
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	eventCounter := 0
	for {
		select {
		// This case is triggered every 2 seconds by the ticker.
		case <-ticker.C:
			eventCounter++
			var eventToSend *stream.Event

			switch eventCounter {
			case 1:
				payload, _ := structpb.NewStruct(map[string]interface{}{"user_id": "usr_123", "status": "active"})
				eventToSend = &stream.Event{EventId: "evt-001", Type: "USER_LOGIN", Payload: payload}
			case 2:
				payload, _ := structpb.NewStruct(map[string]interface{}{"order_id": "ord_abc_987", "value": 99.95})
				eventToSend = &stream.Event{EventId: "evt-002", Type: "ORDER_CREATED", Payload: payload}
			case 3:
				payload, _ := structpb.NewStruct(map[string]interface{}{"latency_ms": 50})
				eventToSend = &stream.Event{EventId: "evt-003", Type: "SYSTEM_PING", Payload: payload}
			default:
				// After sending 3 events, we'll close the stream cleanly.
				log.Println("Finished sending events, closing stream.")
				return nil
			}

			log.Printf("Sending event: %s", eventToSend.Type)
			if err := srv.Send(eventToSend); err != nil {
				log.Printf("Error sending event to client: %v", err)
				return err
			}

		// This case is triggered if the client disconnects.
		case <-srv.Context().Done():
			log.Println("Client disconnected.")
			return nil
		}
	}
}

func main() {
	// Set up a TCP listener on port 50051.
	port := os.Getenv("PORT")
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create a new gRPC server instance.
	s := grpc.NewServer()

	// Register our server implementation with the gRPC server.
	stream.RegisterStreamerServer(s, &server{})

	// **This is the crucial line.** It enables gRPC reflection, allowing
	// tools like grpcurl to query the server for its available services.
	reflection.Register(s)

	log.Println("gRPC server listening at :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
