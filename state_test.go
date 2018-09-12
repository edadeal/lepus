package lepus

import "testing"

var stateStringerCases = []struct {
	name     string
	state    State
	expected string
}{
	{"Unknown", StateUnknown, "Unknown"},
	{"Published", StatePublished, "Published"},
	{"Returned", StateReturned, "Returned"},
	{"Timeout", StateTimeout, "Timeout"},
	{"Closed", StateClosed, "Closed"},
}

func TestStateStringer(t *testing.T) {
	for _, c := range stateStringerCases {
		t.Run(c.name, func(t *testing.T) {
			if c.state.String() != c.expected {
				t.Fatalf("Expecting '%s', got '%s'", c.expected, c.state.String())
			}
		})
	}
}
