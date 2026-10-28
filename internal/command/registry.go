package command

// NewDefaultRegistry returns a registry containing every built-in command.
//
// Registering through one function means the command table, INFO's
// command_count and the CLI's help are all derived from the same source.
func NewDefaultRegistry() *Registry {
	r := NewRegistry()
	registerConnectionCommands(r)
	registerStringCommands(r)
	registerHashCommands(r)
	registerExpiryCommands(r)
	registerKeyspaceCommands(r)
	return r
}
