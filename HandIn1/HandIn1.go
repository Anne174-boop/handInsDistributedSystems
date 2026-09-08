package main

import (
	"fmt"
	"sync"
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
	first, second := ID, (ID+1)%5 //first and second fork for each philosopher
	
	if(ID == 2){
		first, second := second, first//philosopher 2 picks up the right fork first to create asymmetry and avoid deadlock
	}
	
	//creates references between philosopher and the channels of the forks 
	forkTableFirst, forkHandFirst := fork(first)
	forkTableSecond, forkHandSecond := fork(second)

	select {
		case freeFirst <- forkTableFirst: // check status of first fork
			<- forkTableFirst // if free grab it
			fmt.Println("Philosopher " , ID , " has a fork.")
		case freeSecond <- forkTableSecond: //check status of second fork
			<- forkTableSecond // if free grab it
			fmt.Println("Philosopher ", ID, " is now EATING.")
		case notFree <- forkHandFirst: // check status of first fork
			fmt.Println("Philosopher " , ID , " is now THINKING.") 
			return
		}
	
	<-
		
}

func forkTable(ID int) chan int { //fork is free
	forkTable := make(chan int)
	return forkTable
}

func forkHand(ID int) chan int { //fork is not free
	forkHand := make(chan int)
	
	return forkHand
}

// forks are its own thread
func fork(ID int, forkTable chan int, forkHand chan int) { //goroutine for 1 fork
	var free bool = true //fork is free to start with
	if (free == true) {
		forkTable <- "I am free"
	} else {
		forkHand <- "I am taken"
	}
	return forkTable, forkHand
}

first<- philosopher 
second<- philosopher 
first<- true
second<-true //places the forks on the table and sends a message to the philosophers that they are free

// if fork is free <- send to philosopher, but philosopher has to resive 2 messages to the philosophers next to it.
