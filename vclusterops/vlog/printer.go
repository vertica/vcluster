/*
 (c) Copyright [2023] Open Text.
 Licensed under the Apache License, Version 2.0 (the "License");
 You may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package vlog

import (
	"fmt"

	"github.com/go-logr/logr"
)

// Printer is a wrapper for the logger API that handles dual logging to the log
// and stdout. It reimplements all of the APIs from logr but adds additional
// ones to print messages to stdout.
type Printer struct {
	Log           logr.Logger
	LogToFileOnly bool
}

// WithName will construct a new printer with the logger set with an additional
// name. The new printer inherits state from the current Printer.
func (p *Printer) WithName(logName string) Printer {
	return Printer{
		Log:           p.Log.WithName(logName),
		LogToFileOnly: p.LogToFileOnly,
	}
}

// Reimplement the logr APIs that we use. These are simple pass through functions to the logr object.

// V sets the logging level. Can be daisy-chained to produce a log message for
// a given level.
func (p *Printer) V(level int) logr.Logger {
	return p.Log.V(level)
}

// Error displays an error message to the log.
func (p *Printer) Error(err error, msg string, keysAndValues ...any) {
	p.Log.Error(err, msg, keysAndValues...)
}

// Info displays an info message to the log.
func (p *Printer) Info(msg string, keysAndValues ...any) {
	p.Log.Info(msg, keysAndValues...)
}

// APIs to control printing to both the log and standard out.

// PrintInfo will display the given message in the log. And if not logging to
// stdout, it will repeat the message to the console.
func (p *Printer) PrintInfo(msg string, v ...any) {
	fmsg := fmt.Sprintf(msg, v...)
	p.Log.Info(fmsg)
	p.printlnCond(InfoLog, fmsg)
}

// PrintError will display the given error message in the log. And if not
// logging to stdout, it will repeat the message to the console.
func (p *Printer) PrintError(msg string, v ...any) {
	fmsg := fmt.Sprintf(msg, v...)
	p.Log.Error(nil, fmsg)
	p.printlnCond(ErrorLog, fmsg)
}

// PrintWarning will display the given warning message in the log. And if not
// logging to stdout, it will repeat the message to the console.
func (p *Printer) PrintWarning(msg string, v ...any) {
	fmsg := fmt.Sprintf(msg, v...)
	p.Log.Info(fmsg)
	p.printlnCond(WarningLog, fmsg)
}

// printlnCond will conditonally print a message to the console if logging to a file
func (p *Printer) printlnCond(label, msg string) {
	// Message is only printed if we are logging to a file only. Otherwise, it
	// would be duplicated in the log.
	if p.LogToFileOnly {
		fmt.Printf("%s%s\n", label, msg)
	}
}
