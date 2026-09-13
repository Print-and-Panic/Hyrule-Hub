package randomizer

// OotItem is an Ocarina of Time item identifier, as defined by the
// multiworld coop context in emulator memory.
type OotItem uint16

func (i OotItem) String() string {
	return ItemNames[uint16(i)]
}
