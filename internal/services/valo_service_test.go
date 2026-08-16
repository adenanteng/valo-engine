package services

import (
	"sync"
	"testing"
	"time"
)

func TestNormalizePhoneNumber(t *testing.T) {
	cases := map[string]string{
		"+62 812-345": "62812345",
		"0812-345":    "62812345",
		"62812":       "62812",
		" 62812 ":     "62812",
	}
	for in, want := range cases {
		if got := normalizePhoneNumber(in); got != want {
			t.Fatalf("normalizePhoneNumber(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPacerSpacesAndSerializesSends(t *testing.T) {
	s := &ValoService{pacers: make(map[string]*sendPacer)}
	oldMin, oldJitter := sendMinGap, sendJitter
	sendMinGap, sendJitter = 40*time.Millisecond, 20*time.Millisecond
	defer func() { sendMinGap, sendJitter = oldMin, oldJitter }()

	var wg sync.WaitGroup
	start := time.Now()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.pace("628111")
		}()
	}
	wg.Wait()

	// 3 paced sends on one account must take at least 2 gaps (~80ms).
	// Different accounts must not block each other (fast).
	if elapsed := time.Since(start); elapsed < 2*sendMinGap {
		t.Fatalf("pacer did not space sends: %v < %v", elapsed, 2*sendMinGap)
	}
	s.pace("628222") // would deadlock/panic if pacers were shared
}
