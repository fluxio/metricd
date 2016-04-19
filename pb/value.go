package pb

import "fmt"

// StringRep() returns a string representation of the Value.
func (m *Metric) ValueAsString() string {
	switch m.GetValue().(type) {
	case *Metric_IntValue:
		return fmt.Sprint(m.GetIntValue())
	case *Metric_DoubleValue:
		return fmt.Sprint(m.GetDoubleValue())
	}
	return m.GetStringValue()
}

// Returns a float64 representation of the Value.
func (m *Metric) ValueAsFloat() float64 {
	switch m.GetValue().(type) {
	case *Metric_IntValue:
		return float64(m.GetIntValue())
	case *Metric_DoubleValue:
		return m.GetDoubleValue()
	}
	return 0
}
