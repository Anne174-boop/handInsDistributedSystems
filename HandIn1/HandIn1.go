package main

import (
	"fmt"
	"sync"
)

type ForkRequest struct {
    PhilosopherID int
    Take          bool
    Reply         chan bool
}

var forkChans [5]chan ForkRequest //array of channels for each fork
var wg sync.WaitGroup	

// This function creates goroutines for fork an philosophers
// furthermore an array is instantiate and each fork has a channel in this array
// a philosopher uses this channel to either take or return a fork
// using waitgroups to wait for all processes to finish
func main() {

	for i := 0; i < 5; i++ {
		forkChans[i] = make(chan ForkRequest) //creates a channel for each fork

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
func philosopher(ID int, forkChans [5]chan ForkRequest) {
    defer wg.Done()

    first := ID
    second := (ID + 1) % 5

    if ID == 2 {
        first, second = second, first
    }

    eating := 0

    for eating < 3 {

        reply1 := make(chan bool)
        reply2 := make(chan bool)

        forkChans[first] <- ForkRequest{
            PhilosopherID: ID,
            Take:          true,
            Reply:         reply1,
        }

        if !<-reply1 {
            fmt.Println("Philosopher", ID, "is thinking")
            continue
        }

        forkChans[second] <- ForkRequest{
            PhilosopherID: ID,
            Take:          true,
            Reply:         reply2,
        }

        if !<-reply2 {
            forkChans[first] <- ForkRequest{
                PhilosopherID: ID,
                Take:          false,
            }

            fmt.Println("Philosopher", ID, "is thinking")
            continue
        }

        eating++
        fmt.Println("Philosopher", ID, "is eating")

        forkChans[first] <- ForkRequest{
            PhilosopherID: ID,
            Take:          false,
        }

        forkChans[second] <- ForkRequest{
            PhilosopherID: ID,
            Take:          false,
        }
    }

    fmt.Println("Philosopher", ID, "is done eating")
}

// forks are its own thread
func fork(id int, requests chan ForkRequest) {
    available := true

    for {
        req := <-requests

        if req.Take {
            if available {
                available = false
                req.Reply <- true
                fmt.Println("Fork", id, "taken by philosopher", req.PhilosopherID)
            } else {
                req.Reply <- false
            }
        } else {
            available = true
            fmt.Println("Fork", id, "returned by philosopher", req.PhilosopherID)
        }
    }
}
