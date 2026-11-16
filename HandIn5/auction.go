package main

import "time"

// Auction: ren tilstand + regler (ingen netværk)

//definerer en type af Outcome som er heltalsbaseret
type Outcome int
//const erklærer mulighederne for outcome, og den kan kun returnere en af disse:
const ( Success Outcome = iota; Fail; Exception )
//iota er en tæller i Go, som starter ved 0, de næste værdier følger samme mønster
//betyder at succes = 0, fail = 1 og exception = 2

type Auction struct {
	highestBid int;
	highestBidder int; //node-ID
	deadline time.Time; //auktionenslænge -> skal sættes til 10 sek
	bidders map[int]int; //node-ID and latest bid
	seen map[string]Outcome // requestID -> svar (at-most-once)
}

func (a *Auction) Bid(amount int, nodeID int, reqID int, now time.Time) Outcome {
	//exception kastes ved ugyldigt svar -> exception (måske skal det tjek være et andet sted?)

	//map returnere enten out (værdien i map) eller ok(boolean, findes værdien allerede)
	if out, ok := a.seen[reqID]; ok {
		//hvis request er set før, så returnér det gemte resultat
		//hvis ikke springes dette step over, og requesten behandles i stedet
		return out
	}

	var out Outcome
	switch {
	case now.After(a.deadline): //er deadline overskredet? hvis ja -> fail
		out = Fail
		case
		default
	}

	
	//ellers fortsæt
	
	//check om buddet er højere end det nuværende
	//hvis ja -> succes
	//hvis nej -> fail

	//return ack
}

func void result(now time.Time) Outcome {

	//hvis deadline er slut returnere både vinder id og beløb
	//hvis deadline ikke er slut returneres det nuværende højeste bud
}

func Snapshot() {

}

func Restore() {

}