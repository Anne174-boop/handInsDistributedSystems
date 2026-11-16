package main

import "time"

// definerer en type af Outcome som er heltalsbaseret
type Outcome int

// const erklærer mulighederne for outcome, og den kan kun returnere en af disse:
const (
	Success Outcome = iota
	Fail
	Exception
)

//iota er en tæller i Go, som starter ved 0, de næste værdier følger samme mønster
//betyder at succes = 0, fail = 1 og exception = 2

type Auction struct {
	highestBid    int
	highestBidder int             //node-ID
	deadline      time.Time       //auktionenslænge -> skal sættes til 10 sek
	bidders       map[int]int     //node-ID and latest bid
	seen          map[int]Outcome // requestID -> svar (at-most-once)
}

func (a *Auction) SetDeadline(unixNano int64) {
	a.deadline = time.Unix(0, unixNano)
}

func (a *Auction) isOver() bool {
	return time.Now().After(a.deadline)
}

func (a *Auction) Bid(amount int, bidderID int, reqID int) Outcome {
	//exception kastes ved ugyldigt svar -> exception (måske skal det tjek være et andet sted?)

	//map returnere enten out (værdien i map) eller ok(boolean, findes værdien allerede)
	if out, ok := a.seen[reqID]; ok {
		//hvis request er set før, så returnér det gemte resultat
		//hvis ikke springes dette step over, og requesten behandles i stedet
		return out
	}

	//vores retur værdi, enten succes, fail eller exception
	var out Outcome

	switch {
	case amount <= 0:
		out = Exception
	case a.isOver(): //er deadline overskredet? hvis ja -> fail
		out = Fail
	case amount <= a.highestBid: //hvis amount er mindre end eller lig med det nuværende højeste bud -> fail
		out = Fail
	default: //default: her regner vi med at buddet er fint og sætter det som nyt højeste bud
		a.bidders[bidderID] = amount //mapper buddet til bidderID i bidders map
		a.highestBid = amount        //sætter buddet til at være det ledende
		a.highestBidder = bidderID   //sætter noden til at være højeste bidder
		out = Success                //bid var en succes yay
	}

	//gemmer out i seen mappet med tilhørende requestID
	//nu kan vi tjekker om vi har set vores request før i fremtiden
	//out kan enten være succes, fail eller exception
	a.seen[reqID] = out

	return out
}

func (a *Auction) Result() (over bool, winner int, bid int) {
	return a.isOver(), a.highestBidder, a.highestBid
}

// beskriver den nuværende tilstand af auktionen
type SnapshotAuction struct {
	HighestBid    int
	HighestBidder int
	Deadline      time.Time
	Bidders       map[int]int
	Seen          map[int]Outcome
}

func Snapshot(a *Auction) SnapshotAuction {
	//lav kopier af maps UDEN referencer, så de ikke kan ændres hvis noget går tabt
	bidders := make(map[int]int)  //lav nyt map (key, value) -> k=id, v=amount
	for k, v := range a.bidders { //sæt værdierne fra a.bidders ind i det nye map
		bidders[k] = v
	}

	//samme process som ovenfor men med reqID og outcome i stedet
	seen := make(map[int]Outcome)
	for k, v := range a.seen { //k=reqID, v=Outcome(fail/succes)
		seen[k] = v
	}

	//lav snapshot og returner det
	return SnapshotAuction{
		HighestBid:    a.highestBid,
		HighestBidder: a.highestBidder,
		Deadline:      a.deadline,
		Bidders:       bidders,
		Seen:          seen,
	}
}

// når restore() kaldes, læser vi snapshot, og det aktuelle snapshot bruges som argument
// bruger snapshot til at overfører værdierne fra snapshot tilbage til auction hvis noget er crashet
func Restore(a *Auction, s SnapshotAuction) {
	a.highestBid = s.HighestBid
	a.highestBidder = s.HighestBidder
	a.deadline = s.Deadline
	a.bidders = s.Bidders
	a.seen = s.Seen
}
