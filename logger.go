package pgoutput

// Logger is our contract for the logger
type Logger interface {
	Debugf(format string, args ...interface{})
}
