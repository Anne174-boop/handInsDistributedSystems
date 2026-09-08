package main

import (
	"fmt"
	"sync"
)

var forkChans [5]chan bool
var wg sync.WaitGroup

// This function creates goroutines for fork an philosophers
// furthermore an array is instantiate and each fork has a channel in this array
// a philosopher uses this channel to either take or return a fork
// using waitgroups to wait for all processes to finish
func main() {

	for i := 0; i < 5; i++ {
		forkChans[i] = make(chan bool, 1) //creates a channel for each fork

		go fork(i, forkChans[i]) //goroutine fork, program doesn't wait, routine runs when it can
	}

	//for-loop runs 5 times for 5 philosophers and gives them their ID (0-4)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go philosopher(i, forkChans) //goroutine philosopher, program doesn't wait, routine runs when it can
	}

	// Wait until all philosophers are done eating
	wg.Wait()
	fmt.Println("Amen, dinner is over and all philosophers are now full<3")
}

// goroutine for 1 philosopher
// gets an ID so we can differentiate between philosophers
func philosopher(ID int, forkChans [5]chan bool) {
	defer wg.Done() //do this when everything is done

	first := ID            //first fork for a philosopher
	second := (ID + 1) % 5 //second fork for a philosopher

	//creating asymmetry and avoid deadlock
	if ID == 2 {
		first, second = second, first
	}

	//a philosopher has eaten 0 times at first
	eating := 0

	for eating != 3 {
		//creates references between philosopher and the channels of the forks
		forkChanFirst := forkChans[first]
		forkChanSecond := forkChans[second]

		var hasFork1 bool
		var hasFork2 bool

		select {
		case <-forkChanFirst: // check status of first fork
			fmt.Println("Philosopher ", ID, " has a fork.")
			hasFork1 = true
		default:
			fmt.Println("Philosopher ", ID, " is thinking.") // happens if a philosopher has no forks
			continue
		}

		select {
		case <-forkChanSecond:
			fmt.Println("Philosopher ", ID, " has another fork")
			hasFork2 = true
		default:
			fmt.Println("Philosopher ", ID, " is thinking.") // happens if a philosopher has 1 or 0 forks
			if hasFork1 {
				forkChanFirst <- true
			}
			continue
		}

		if hasFork1 && hasFork2 {
			eating++
			fmt.Println("Philosopher ", ID, " is eating.") // happens if a philosopher has 2 forks
			forkChanFirst <- true
			forkChanSecond <- true
		}
	}
	fmt.Println("Philosopher ", ID, " is done eating.") // happens if a philosopher has 1 or 0 forks

}

// forks are its own thread
func fork(ID int, forkChan chan bool) { //goroutine for 1 fork
	//places the fork on the table and sends a message to the philosophers that it is free
	forkChan <- true
	fmt.Println("Fork ", ID, " is on the table.")
}
