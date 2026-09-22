package coordinator

import (
	"fmt"
	"io"
	"sync"
)

// console owns the one line a restore rewrites in place while it runs. Everything else
// the restore has to say goes through here too, so a message lands above that line
// rather than through the middle of it, and the line is drawn again whole afterwards.
//
// The line and the messages go to different writers because they are read differently.
// The line is for someone watching and is overwritten as soon as it is stale, so it is
// worth nothing once the restore ends. A message is a record of something that happened
// and has to survive being piped to a file.
//
// Every write is serialised, because both pools report failures while the reporter is
// rewriting the line.
type console struct {
	progress io.Writer
	messages io.Writer
	mu       sync.Mutex
	width    int // Characters the line now on screen occupies
}

// newConsole returns a console writing its progress line to one writer and its messages
// to another.
func newConsole(progress, messages io.Writer) *console {
	return &console{progress: progress, messages: messages}
}

// update replaces the line on screen with this one. A line shorter than its predecessor
// is padded, so nothing of the old one shows through.
func (c *console) update(line string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// The console is the last place a write error could be reported, so there is
	// nowhere for one to go.
	_, _ = fmt.Fprintf(c.progress, "\r%-*s", c.width, line)
	c.width = len(line)
}

// line prints a line that stays, alongside the progress line rather than instead of it.
// It is for what a restore reports as it goes: what it verified, what it wrote, where
// the report went.
func (c *console) line(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eraseLocked()
	_, _ = fmt.Fprintf(c.progress, format+"\n", args...)
}

// notice prints a message that stays, on the writer messages go to. The progress line is erased first and left
// erased, so the next update draws it again below the message rather than the message
// landing in the middle of it.
func (c *console) notice(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.eraseLocked()
	_, _ = fmt.Fprintf(c.messages, format+"\n", args...)
}

// done finishes the progress line, so whatever is printed next starts on its own line.
func (c *console) done() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.width > 0 {
		_, _ = fmt.Fprintln(c.progress)
		c.width = 0
	}
}

// eraseLocked blanks the line on screen and returns the cursor to its start.
func (c *console) eraseLocked() {
	if c.width == 0 {
		return
	}
	_, _ = fmt.Fprintf(c.progress, "\r%-*s\r", c.width, "")
	c.width = 0
}
