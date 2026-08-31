package main

import (
	"fmt"
)

func main() {
	fmt.Println("CHANGE IN STATE FOR FORK OR PHILOSOPHER")
	forkChannel := channel()
	go philosopher(forkChannel)
}

func channel() chan int {
	forkChannel := make(chan int, 1) //buffered channel for forks
	return forkChannel
}

// goroutine for all philosophers (5)
func philosopher(forkChannel chan int) {
	select {
	case <-forkChannel:
		forkChannel <- 1 //take fork
	default:
		fmt.Println("Philosopher is waiting for fork")
	}

}

// goroutine for all forks (5)
// forks are its own thread
func fork() {

}
