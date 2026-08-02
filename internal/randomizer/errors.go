package randomizer

import "fmt"

type BlockedSemaphoreError struct {
	PlayerID uint8
	Item     OotItem
}

func (b *BlockedSemaphoreError) Error() string {
	return fmt.Sprintf("semaphore is blocked by player %d with item %s", b.PlayerID, b.Item)
}

func (b *BlockedSemaphoreError) Retryable() bool {
	return true
}
