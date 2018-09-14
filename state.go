package lepus

import "strconv"

// State indicates publishing state of message
type State int32

// states
const (
	StateUnknown          State = iota // Unknown
	StatePublished                     // Published
	StateReturned                      // Returned
	StateTimeout                       // Timeout
	StateClosed                        // Closed
	StateAlreadyProcessed              // AlreadyProcessed
)

const _State_name = "UnknownPublishedReturnedTimeoutClosedAlreadyProcessed"

var _State_index = [...]uint8{0, 7, 16, 24, 31, 37, 53}

func (i State) String() string {
	if i < 0 || i >= State(len(_State_index)-1) {
		return "State(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _State_name[_State_index[i]:_State_index[i+1]]
}
