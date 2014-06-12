package loglib

const (
	DEBUG   = 0
	INFO    = 1
	WARNING = 2
	ERROR   = 3
)

var prefixes = []string{"DEBUG", "INFO", "WARNING", "ERROR"}

type Log interface {
	SetLevel(level int) bool
	Debug(msg string)
	Info(msg string)
	Warning(msg string)
	Error(msg string)
}
