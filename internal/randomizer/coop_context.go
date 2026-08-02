package randomizer

const (
	coopContextAddress = 0x8040_0000
	coopContextSize    = 0xc24
)

const (
	VersionOffset                  = 0x0000
	PlayerIDOffset                 = 0x0004
	PlayerNameIDOffset             = 0x0005
	IncomingPlayerOffset           = 0x0006
	IncomingItemOffset             = 0x0008
	MWSendOwnItemsOffset           = 0x000a
	MWProgressiveItemsEnableOffset = 0x000b
	OutgoingItemOffset             = 0x0010
	OutgoingPlayerOffset           = 0x0012
	PlayerNamesOffset              = 0x0014
	CfgFileSelectHashOffset        = 0x0814
	MWProgressiveItemsStateOffset  = 0x081c
	OutgoingKeyOffset              = 0x0c1c
)

type CoopContext struct {
	Version                  uint32
	PlayerID                 uint8
	PlayerNameID             uint8
	IncomingPlayer           uint16
	IncomingItem             OotItem
	MWSendOwnItems           uint8
	MWProgressiveItemsEnable uint8
	OutgoingItem             OotItem
	OutgoingPlayer           uint16
	PlayerNames              []string
	CFGFileSelectHash        []HashIcon
	MWProgressiveItemsState  [][]byte
	OutgoingKey              uint64
}

type HashIcon byte

func HashIconFromByteArray(bytes []byte) []HashIcon {
	result := make([]HashIcon, len(bytes))
	for i, b := range bytes {
		result[i] = HashIcon(b)
	}
	return result
}

func (h HashIcon) String() string {
	return hashIconMap[byte(h)]
}

var hashIconMap map[byte]string

func init() {
	hashIconMap = map[byte]string{
		0:  "Deku Stick",
		1:  "Deku Nut",
		2:  "Bow",
		3:  "Slingshot",
		4:  "Fairy Ocarina",
		5:  "Bombchu",
		6:  "Longshot",
		7:  "Boomerang",
		8:  "Lens of Truth",
		9:  "Beans",
		10: "Megaton Hammer",
		11: "Bottled Fish",
		12: "Bottled Milk",
		13: "Mask of Truth",
		14: "SOLD OUT",
		15: "Cucco",
		16: "Mushroom",
		17: "Saw",
		18: "Frog",
		19: "Master Sword",
		20: "Mirror Shield",
		21: "Kokiri Tunic",
		22: "Hover Boots",
		23: "Silver Gauntlets",
		24: "Gold Scale",
		25: "Stone of Agony",
		26: "Skull Token",
		27: "Heart Container",
		28: "Boss Key",
		29: "Compass",
		30: "Map",
		31: "Big Magic",
	}

}
