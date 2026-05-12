package relay

import "fmt"

type TimeoutStrategy int

const (
	// StrategyNone: no timeout enforcement, all objects reliable.
	StrategyNone TimeoutStrategy = iota

	// StrategyResetStream: on timeout, reset the subgroup and drop the rest
	// of the group.
	StrategyResetStream

	// StrategyResetAtObject: RESET_STREAM_AT after every completed object;
	// objects written before the boundary survive a timeout.
	StrategyResetAtObject

	// StrategyResetAtKeyframe: RESET_STREAM_AT only after the I-frame, so
	// the I-frame is preserved but later P/B-frames may be lost.
	StrategyResetAtKeyframe
)

func (s TimeoutStrategy) String() string {
	switch s {
	case StrategyNone:
		return "none"
	case StrategyResetStream:
		return "reset-stream"
	case StrategyResetAtObject:
		return "reset-at-object"
	case StrategyResetAtKeyframe:
		return "reset-at-keyframe"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

func ParseStrategy(s string) (TimeoutStrategy, error) {
	switch s {
	case "none":
		return StrategyNone, nil
	case "reset-stream":
		return StrategyResetStream, nil
	case "reset-at-object":
		return StrategyResetAtObject, nil
	case "reset-at-keyframe":
		return StrategyResetAtKeyframe, nil
	default:
		return StrategyNone, fmt.Errorf("unknown strategy: %q (valid: none, reset-stream, reset-at-object, reset-at-keyframe)", s)
	}
}
