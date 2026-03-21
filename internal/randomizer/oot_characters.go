package randomizer

var ootCharset = map[rune]byte{
	'0': 0x00, '1': 0x01, '2': 0x02, '3': 0x03, '4': 0x04,
	'5': 0x05, '6': 0x06, '7': 0x07, '8': 0x08, '9': 0x09,
	'A': 0xAB, 'B': 0xAC, 'C': 0xAD, 'D': 0xAE, 'E': 0xAF,
	'F': 0xB0, 'G': 0xB1, 'H': 0xB2, 'I': 0xB3, 'J': 0xB4,
	'K': 0xB5, 'L': 0xB6, 'M': 0xB7, 'N': 0xB8, 'O': 0xB9,
	'P': 0xBA, 'Q': 0xBB, 'R': 0xBC, 'S': 0xBD, 'T': 0xBE,
	'U': 0xBF, 'V': 0xC0, 'W': 0xC1, 'X': 0xC2, 'Y': 0xC3, 'Z': 0xC4,
	'a': 0xC5, 'b': 0xC6, 'c': 0xC7, 'd': 0xC8, 'e': 0xC9,
	'f': 0xCA, 'g': 0xCB, 'h': 0xCC, 'i': 0xCD, 'j': 0xCE,
	'k': 0xCF, 'l': 0xD0, 'm': 0xD1, 'n': 0xD2, 'o': 0xD3,
	'p': 0xD4, 'q': 0xD5, 'r': 0xD6, 's': 0xD7, 't': 0xD8,
	'u': 0xD9, 'v': 0xDA, 'w': 0xDB, 'x': 0xDC, 'y': 0xDD, 'z': 0xDE,
	' ': 0xDF, '-': 0xE4, '.': 0xE8,
}

// EncodeOoTName translates a Go string into a padded 8-byte OoT array
func EncodeOoTName(name string) [8]byte {
	var out [8]byte

	// Pre-fill the array with the OoT "Space" character (0xDF)
	// This prevents names from ending in "000"
	for i := range out {
		out[i] = 0xDF
	}

	// Translate up to 8 characters
	for i, char := range name {
		if i >= 8 {
			break // OoT names have a hard limit of 8 characters
		}
		if ootByte, ok := ootCharset[char]; ok {
			out[i] = ootByte
		} else {
			out[i] = 0xDF // Fallback to a space if we have an unsupported character
		}
	}
	return out
}
