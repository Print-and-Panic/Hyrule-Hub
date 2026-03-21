package randomizer

import "fmt"

type IncomingItem struct {
	World uint16  `json:"world"`
	Item  OotItem `json:"item"`
	Key   uint64  `json:"key"`
}

type OutgoingItem struct {
	World       uint16  `json:"world"`
	Item        OotItem `json:"item"`
	OutgoingKey uint64  `json:"outgoingKey"`
}

func (o OutgoingItem) String() string {
	return fmt.Sprintf("World: %d, Item: %s, OutgoingKey: %d", o.World, o.Item, o.OutgoingKey)
}

type OotItem uint16

func (i OotItem) String() string {
	return ItemNames[uint16(i)]
}
