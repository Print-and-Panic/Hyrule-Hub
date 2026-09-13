// Command faken64 is a development harness that emulates just enough of
// RetroArch's network command interface (UDP READ_CORE_MEMORY /
// WRITE_CORE_MEMORY) and the OoT multiworld coop context to exercise
// Hyrule-Hub end-to-end without a ROM.
//
// It simulates a running game:
//   - after a boot delay it "allocates" the coop context by planting its
//     pointer at 0x80400000
//   - every -interval it sets a fake outgoing item for -target world and
//     waits for the client to publish + clear the semaphore
//   - it polls the incoming semaphore and "consumes" grants, logging the
//     item name and source world the way the game would display them
//
// Example (two players on one machine):
//
//	faken64 -addr 127.0.0.1:55355 -world 1 -target 2
//	faken64 -addr 127.0.0.1:55356 -world 2 -target 1
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/Print-and-Panic/Hyrule-Hub/internal/randomizer"
)

// Coop context layout, matching internal/bridge/retroarch. The fake ROM
// "allocates" the context at 0x80401000.
const (
	coopContextPointerAddr = 0x8040_0000 // where the ROM publishes the pointer
	coopContextBase        = 0x8040_1000 // where the ROM "allocated" the context

	playerIDOffset     = 0x0004
	outgoingItemOffset = 0x0010
	outgoingKeyOffset  = 0x0c1c
)

// fakeItems is a pool of item IDs the fake player "finds" in other worlds.
var fakeItems = []uint16{0x0029, 0x0051, 0x003D, 0x0067, 0x0075, 0x00A9}

// console is fake physical memory plus the byte-order conventions of the
// Mupen64Plus core. The network command interface speaks in word-swapped
// wire order; the game simulation reads/writes in N64 order.
type console struct {
	mu  sync.Mutex
	mem map[uint32]byte
}

func toPhysical(vAddr uint32) uint32 { return vAddr & 0x00FFFFFF }

func unSwap(data []byte) {
	for i := 0; i+3 < len(data); i += 4 {
		data[i], data[i+1], data[i+2], data[i+3] = data[i+3], data[i+2], data[i+1], data[i]
	}
}

// readN64 returns size bytes at an N64 virtual address in N64 byte order.
func (c *console) readN64(vAddr uint32, size uint32) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]byte, size)
	phys := toPhysical(vAddr)
	for i := range out {
		out[i] = c.mem[phys+uint32(i)]
	}
	unSwap(out)
	return out
}

// writeN64 stores data (N64 byte order) at an N64 virtual address.
func (c *console) writeN64(vAddr uint32, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	buf := append([]byte(nil), data...)
	unSwap(buf)
	phys := toPhysical(vAddr)
	for i, b := range buf {
		c.mem[phys+uint32(i)] = b
	}
}

// serveWire handles RetroArch network commands. Wire bytes are stored
// verbatim (already word-swapped by the client side).
func (c *console) serveWire(conn *net.UDPConn) {
	buf := make([]byte, 65535)
	for {
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		c.handle(conn, remote, strings.TrimSpace(string(buf[:n])))
	}
}

func (c *console) handle(conn *net.UDPConn, remote *net.UDPAddr, cmd string) {
	fields := strings.Fields(cmd)
	if len(fields) < 2 {
		return
	}

	var phys uint32
	fmt.Sscanf(fields[1], "%x", &phys)

	switch fields[0] {
	case "READ_CORE_MEMORY":
		var size uint32
		fmt.Sscanf(fields[2], "%d", &size)

		c.mu.Lock()
		parts := make([]string, size)
		for i := range size {
			parts[i] = fmt.Sprintf("%02x", c.mem[phys+i])
		}
		c.mu.Unlock()

		resp := fmt.Sprintf("READ_CORE_MEMORY %x %s", phys, strings.Join(parts, " "))
		conn.WriteToUDP([]byte(resp), remote)

	case "WRITE_CORE_MEMORY":
		c.mu.Lock()
		for i, hexByte := range fields[2:] {
			var b byte
			fmt.Sscanf(hexByte, "%02x", &b)
			c.mem[phys+uint32(i)] = b
		}
		c.mu.Unlock()
		conn.WriteToUDP([]byte(fmt.Sprintf("WRITE_CORE_MEMORY %x", phys)), remote)
	}
}

// simulate runs the "game": boot, periodically generate outgoing items,
// and consume incoming grants.
func simulate(c *console, world, target uint16, interval, bootDelay time.Duration) {
	time.Sleep(bootDelay)

	var ptr [4]byte
	binary.BigEndian.PutUint32(ptr[:], coopContextBase)
	c.writeN64(coopContextPointerAddr, ptr[:])
	log.Printf("[game] ROM booted, coop context at %#08x", coopContextBase)

	genTick := time.NewTicker(interval)
	pollTick := time.NewTicker(50 * time.Millisecond)

	var pendingKey uint64
	nextKey := uint64(1)
	itemIdx := 0

	for {
		select {
		case <-genTick.C:
			if pendingKey != 0 {
				continue // client hasn't published the last one yet
			}
			item := fakeItems[itemIdx%len(fakeItems)]
			itemIdx++

			var itemBuf [4]byte
			binary.BigEndian.PutUint16(itemBuf[0:2], item)
			binary.BigEndian.PutUint16(itemBuf[2:4], target)
			c.writeN64(coopContextBase+outgoingItemOffset, itemBuf[:])

			var keyBuf [8]byte
			binary.BigEndian.PutUint64(keyBuf[:], nextKey)
			c.writeN64(coopContextBase+outgoingKeyOffset, keyBuf[:])

			pendingKey = nextKey
			nextKey++
			log.Printf("[game] found %s for world %d (key %d), waiting for client...",
				randomizer.ItemNames[item], target, pendingKey)

		case <-pollTick.C:
			// Did the client publish and clear our outgoing item?
			if pendingKey != 0 {
				key := binary.BigEndian.Uint64(c.readN64(coopContextBase+outgoingKeyOffset, 8))
				if key == 0 {
					log.Printf("[game] item sent to world %d (key %d acknowledged)", target, pendingKey)
					pendingKey = 0
				}
			}

			// Consume any incoming grant the client wrote.
			block := c.readN64(coopContextBase+playerIDOffset, 8)
			fromWorld := binary.BigEndian.Uint16(block[2:4])
			item := binary.BigEndian.Uint16(block[4:6])
			if fromWorld == 0 && item == 0 {
				continue
			}
			log.Printf("[game] >>> received %s from world %d! <<<",
				randomizer.ItemNames[item], fromWorld)

			for i := 2; i < 6; i++ {
				block[i] = 0
			}
			c.writeN64(coopContextBase+playerIDOffset, block)
		}
	}
}

func main() {
	addr := flag.String("addr", "127.0.0.1:55355", "UDP listen address")
	world := flag.Uint("world", 1, "this fake player's world number")
	target := flag.Uint("target", 2, "world to send generated items to")
	interval := flag.Duration("interval", 10*time.Second, "how often to generate an outgoing item")
	bootDelay := flag.Duration("boot", time.Second, "delay before the coop context pointer appears")
	flag.Parse()

	udpAddr, err := net.ResolveUDPAddr("udp", *addr)
	if err != nil {
		log.Fatalf("invalid addr: %v", err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	defer conn.Close()

	c := &console{mem: make(map[uint32]byte)}

	log.Printf("[faken64] world %d listening on %s, sending items to world %d every %s",
		*world, *addr, *target, *interval)

	go c.serveWire(conn)
	simulate(c, uint16(*world), uint16(*target), *interval, *bootDelay)
}
