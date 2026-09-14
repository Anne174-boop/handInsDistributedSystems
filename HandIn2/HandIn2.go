package main

import (
	"fmt"
	"sync"
)

var wg sync.WaitGroup

// a 'new' struct called message, which has two values
// this allows us to send both seq and ack back and forth between server and client
type Message struct {
	seq int
	ack int
}

func main() { //TCP handshake using thread
	clientChan := make(chan Message) //the channel which we send the counter back and forth with
	serverChan := make(chan Message) //the channel which we send the counter forth and back with

	wg.Add(2) //adds two jobs to the waitgroup taskCounter -> thus we wait on these to finish
	go server(clientChan, serverChan)
	go client(clientChan, serverChan)

	wg.Wait()
}

func client(clientChan chan Message, serverChan chan Message) {
	defer wg.Done() //do this when everything is done

	sequenceX := 100                              //starts at this number and increments for each step in the protocol
	clientChan <- Message{seq: sequenceX, ack: 0} //send the sequence to the server (SYN -> let's talk)

	message := <-serverChan //receive the sequence from the server
	fmt.Println("Client received seq: ", message.seq, " and ack: ", message.ack)

	seq := message.ack                        //ack = y + 1 -> in our case y=300
	ack := message.seq + 1                    //seq = x + 1 -> in our case x=100
	clientChan <- Message{seq: seq, ack: ack} //send the sequence to the server (ACK -> let's talk and I acknowledge your sequence)
}

func server(clientChan chan Message, serverChan chan Message) {
	defer wg.Done() //do this when everything is done

	message := <-clientChan //receive the sequence from the client
	fmt.Println("Server received seq: ", message.seq, " and ack: ", message.ack)
	ack := message.seq + 1 //ACK -> next expected sequence number

	sequenceY := 300 //start the sequence at 200, to see the difference in sqX and sqY

	serverChan <- Message{seq: sequenceY, ack: ack} //send the sequence to the client (SYN-ACK -> let's talk and I acknowledge your sequence)

	message = <-clientChan //receive the sequence from the client
	fmt.Println("Server received seq: ", message.seq, " and ack: ", message.ack)
}
