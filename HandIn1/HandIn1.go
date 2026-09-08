package main

import (
	"fmt"
	"sync"
)

var forkChans[5] chan bool
var wg sync.WaitGroup

// This function creates goroutines for fork an philosophers
// furthermore an array is instantiate and each fork has a channel in this array
// a philosopher uses this channel to either take or return a fork
// using waitgroups to wait for all processes to finish
func main() {
	
	for i := 0 ; i<5 ; i++ {
		forkChans[i] = make(chan bool) //creates a channel for each fork
		
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
func philosopher(ID int, forkChans[5] chan bool) {
	defer wg.Done() //do this when everything is done

	first :=ID //first fork for a philosopher
	second := (ID+1)%5 //second fork for a philosopher

	//creating asymmetry and avoid deadlock
	if ID == 2 {
		first, second = second, first 
	}

	//a philosopher has eaten 0 times at first
	eating := 0;

	for eating != 3 {
		//creates references between philosopher and the channels of the forks
		forkChanFirst := forkChans[first]
		forkChanSecond := forkChans[second]

		select {
			case <- forkChanFirst: // check status of first fork
				<- forkChanFirst // if true/free grab it, take fork
				
				fmt.Println("Philosopher ", ID, " has a fork.")
			
			default:
				fmt.Println("Philosopher ", ID, " is thinking.") // happens if a philosopher has no forks
				break
		}

		select {
			case <- forkChanSecond: 
				<- forkChanSecond 

				fmt.Println("Philosopher ", ID, " is eating.")
				eating++
			default:
				fmt.Println("Philosopher ", ID, " is thinking.") // happens if a philosopher has no forks
				break	
		}

		forkChanFirst <- true
		forkChanSecond <- true
	} 
	
}

func forkChan(ID int) chan bool { //fork is free
	forkChan := make(chan int)
	return forkChan
}

// forks are its own thread
func fork(ID int, forkChan chan bool) { //goroutine for 1 fork
	var free bool = true //fork is free to start with
	if free == true {
		forkChan <- 1 //places the fork on the table and sends a message to the philosophers that it is free
		fmt.Println("Fork ", ID, " is on the table.")
	}
}

/*
first<- philosopher
second<- philosopher
first<- true
second<-true //places the forks on the table and sends a message to the philosophers that they are free

// if fork is free <- send to philosopher, but philosopher has to resive 2 messages to the philosophers next to it.
*/
