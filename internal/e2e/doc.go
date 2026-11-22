// Package e2e holds the end-to-end tests of reliable communications over
// real UDP sockets: two gateway processes (two event store handles on one
// database file), the command dispatcher, the alert consumer, simulated
// devices on their own sockets, and the chronological processor and fusion
// engine downstream. It has no production code.
package e2e
