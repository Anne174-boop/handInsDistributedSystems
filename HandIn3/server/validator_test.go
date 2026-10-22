package main

import (
	"strings"
	"testing"
)

// t er parameterens navn.
// testing.T er en type, der bruges til at rapportere testfejl og logge testoutput.
func TestValidateMessage(t *testing.T) {
	// Test case 1: Valid message
	validMessage := "Hello, World!"
	if !validateMessage(validMessage) {
		t.Errorf("Expected true for valid message, got false")
	}

	// Test case 2: Message with more than 128 Unicode code points
	longMessage := strings.Repeat("a", 129)
	if validateMessage(longMessage) {
		t.Errorf("Expected false for long message, got true")
	}

	// Test case 3: Message with invalid UTF-8
	invalidUTF8 := "\xff\xfe\xfd"
	if validateMessage(invalidUTF8) {
		t.Errorf("Expected false for invalid UTF-8 message, got true")
	}
	// Præcis 128 tegn skal accepteres.
	if !validateMessage(strings.Repeat("a", 128)) {
		t.Error("Expected true for 128 characters")
	}
	// Danske bogstaver skal tælles som kodepunkter, ikke bytes.
	if !validateMessage(strings.Repeat("æ", 128)) {
		t.Error("Expected true for 128 Danish characters")
	}
}
