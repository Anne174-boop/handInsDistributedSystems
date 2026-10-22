package main

import (
	"fmt"
	"unicode/utf8"
)

func validateMessage(message string) bool {
	// Afvis beskeder med mere end 128 Unicode-kodepunkter.
	if utf8.RuneCountInString(message) > 128 {
		fmt.Println("Message exceeds 128 characters.")
		return false
	}
	// Afvis tekst, der ikke er gyldig UTF-8.
	if utf8.ValidString(message) == false {
		fmt.Println("Message contains invalid UTF-8 characters.")
		return false
	}
	return true
}
