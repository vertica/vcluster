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
	"os"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
)

const (
	InfoLog    = "[INFO] "
	WarningLog = "[WARNING] "
	ErrorLog   = "[ERROR] "
	DebugLog   = "[DEBUG] "
)

// Printer is a wrapper for the logger API that handles dual logging to the log
// and stdout. It reimplements all of the APIs from logr but adds two additional
// members: one is for printing messages to stdout, and the other one is for identifying
// where the logger came from.
type Printer struct {
	Log           logr.Logger
	LogToFileOnly bool
	// ForCli can indicate if vclusterops is called from vcluster cli or other clients
	ForCli bool
}

// WithName will construct a new printer with the logger set with an additional
// name. The new printer inherits state from the current Printer.
func (p *Printer) WithName(logName string) Printer {
	return Printer{
		Log:           p.Log.WithName(logName),
		LogToFileOnly: p.LogToFileOnly,
		ForCli:        p.ForCli,
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
	escapedFmsg := escapeSpecialCharacters(fmsg)
	p.Log.Info(escapedFmsg)
	p.printlnCond(InfoLog, fmsg)
}

// PrintError will display the given error message in the log. And if not
// logging to stdout, it will repeat the message to the console.
func (p *Printer) PrintError(msg string, v ...any) {
	fmsg := fmt.Sprintf(msg, v...)
	escapedFmsg := escapeSpecialCharacters(fmsg)
	p.Log.Error(nil, escapedFmsg)
	p.printlnCond(ErrorLog, fmsg)
}

// PrintWarning will display the given warning message in the log. And if not
// logging to stdout, it will repeat the message to the console.
func (p *Printer) PrintWarning(msg string, v ...any) {
	fmsg := fmt.Sprintf(msg, v...)
	escapedFmsg := escapeSpecialCharacters(fmsg)
	p.Log.Info(escapedFmsg)
	p.printlnCond(WarningLog, fmsg)
}

// escapeSpecialCharacters will escape special characters (tabs or newlines) in the message.
// Messages that are typically meant for the console could have tabs and newlines for alignment.
// We want to escape those when writing the message to the log so that each log entry is exactly one line long.
func escapeSpecialCharacters(message string) string {
	message = strings.ReplaceAll(message, "\n", "\\n")
	message = strings.ReplaceAll(message, "\t", "\\t")
	return message
}

// printlnCond will conditonally print a message to the console if logging to a file
func (p *Printer) printlnCond(label, msg string) {
	// Message is only printed if we are logging to a file only. Otherwise, it
	// would be duplicated in the log.
	if p.LogToFileOnly {
		fmt.Printf("%s%s\n", label, msg)
	}
}

// log functions for specific cases.
func (p *Printer) LogArgParse(inputArgv *[]string) {
	fmsg := fmt.Sprintf("Called method Parse with args: %q.", *inputArgv)
	p.Log.Info(fmsg)
}

// log functions with masked params
func (p *Printer) LogMaskedArgParse(inputArgv []string) {
	maskedPairs := logMaskedArgParseHelper(inputArgv)
	fmsg := fmt.Sprintf("Called method Parse with args: %q.", maskedPairs)
	p.Log.Info(fmsg)
}

func logMaskedArgParseHelper(inputArgv []string) (maskedPairs []string) {
	sensitiveKeyParams := map[string]bool{
		"awsauth":                 true,
		"awssessiontoken":         true,
		"gcsauth":                 true,
		"azurestoragecredentials": true,
	}
	const (
		expectedParts = 2
		maskedValue   = "******"
	)
	// We need to mask any parameters containing sensitive information
	// with value format k=v,k=v,k=v...
	targetMaskedArg := map[string]bool{
		"--config-param": true,
	}
	// some params have simple value format v
	targetMaskedSimpleArg := map[string]bool{
		"--password": true,
	}

	for i := 0; i < len(inputArgv); i++ {
		if targetMaskedArg[inputArgv[i]] && i+1 < len(inputArgv) {
			pairs := strings.Split(inputArgv[i+1], ",")
			for _, pair := range pairs {
				keyValue := strings.Split(pair, "=")
				if len(keyValue) == expectedParts {
					key := keyValue[0]
					value := keyValue[1]
					keyLowerCase := strings.ToLower(key)
					if sensitiveKeyParams[keyLowerCase] {
						value = maskedValue
					}
					maskedPairs = append(maskedPairs, inputArgv[i], key+"="+value)
				} else {
					// Handle invalid  format
					maskedPairs = append(maskedPairs, pair)
				}
			}
			i++ // Skip the next arg since it has been masked
		} else if targetMaskedSimpleArg[inputArgv[i]] && i+1 < len(inputArgv) {
			maskedPairs = append(maskedPairs, inputArgv[i], maskedValue)
			i++ // Skip the next arg since it has been masked
		} else {
			maskedPairs = append(maskedPairs, inputArgv[i])
		}
	}
	return maskedPairs
}

// setupOrDie will setup the logging for vcluster CLI. One exit, p.Log will
// be set.
func (p *Printer) SetupOrDie(logFile string) {
	// The vcluster library uses logr as the logging API. We use Uber's zap
	// package to implement the logging API.
	EncoderConfigWithoutCaller := zap.NewDevelopmentEncoderConfig()
	EncoderConfigWithoutCaller.EncodeCaller = nil // Set EncodeCaller to nil to exclude caller information
	cfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(zap.InfoLevel),
		Development: false,
		// Sampling is enabled at 100:100, meaning that after the first 100 log
		// entries with the same level and message in the same second, it will
		// log every 100th entry with the same level and message in the same second.
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "console",
		EncoderConfig:    EncoderConfigWithoutCaller,
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}
	// If no log file is given, we just log to standard output
	if logFile != "" {
		p.LogToFileOnly = true
		cfg.OutputPaths = []string{logFile}
	}
	zapLg, err := cfg.Build()
	if err != nil {
		fmt.Printf("Failed to setup the logger: %s", err.Error())
		os.Exit(1)
	}
	p.Log = zapr.NewLogger(zapLg)
	p.Log.Info("Successfully started logger", "logFile", logFile)
}

// PrintWithIndent prints message to console only with an indentation
func (p *Printer) PrintWithIndent(msg string, v ...any) {
	if p.ForCli {
		// the indent level may be adjusted
		const indentLevel = 2
		fmt.Printf("%*s%s\n", indentLevel, "", fmt.Sprintf(msg, v...))
	}
}
