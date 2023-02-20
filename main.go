package main

import "broker/pubsub"

func main() {
	pubSub := pubsub.BuildPubSub(":8080", ":8085", []string{"sports", "food", "play"})
	go pubSub.HandleRegistration()
	pubSub.AcceptMessages()
}
