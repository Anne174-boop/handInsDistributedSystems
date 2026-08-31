package main

import (
	"fmt"
)

func main() {
	//for-loop runs 5 times for 5 philosophers and gives them their ID (0-4)
	for i := 0; i < 5; i++ {
		go philosopher(i)                     //goroutine philosopher, program doesn't wait, routine runs when it can
		go fork(i, forkTable(i), forkHand(i)) //goroutine fork, program doesn't wait, routine runs when it can
	}

}

// goroutine for 1 philosopher
// gets an ID so we can differentiate between philosophers
func philosopher(ID int) {
	fmt.Println(ID)

}

func forkTable(ID int) chan int {
	forkTable := make(chan int)
	return forkTable
}

func forkHand(ID int) chan int {
	forkHand := make(chan int)
	return forkHand
}

// forks are its own thread
func fork(ID int, forkTable chan int, forkHand chan int) {

}

// if fork is free <- send to philosopher, but philosopher has to resive 2 messages to the philosophers next to it.
