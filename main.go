package main

import "broker/pubsub"

func main() {
	//start up with default topics
	pubSub := pubsub.BuildPubSub(":8080", ":8085", []string{"sports", "food", "play"})
	go pubSub.HandleRegistration()
	go pubSub.SignalHandler()
	pubSub.AcceptMessages()
}
