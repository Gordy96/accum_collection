package accumulator

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestOne(t *testing.T) {
	a := NewAccumulator[string](200*time.Millisecond, 0)
	defer a.Stop()
	var res []string
	a.OnReady(func(d []string) {
		res = d
	})
	a.Enqueue("one")
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, res)
	time.Sleep(150 * time.Millisecond)
	assert.ElementsMatch(t, res, []string{"one"})
}

func TestSequence(t *testing.T) {
	a := NewAccumulator[string](200*time.Millisecond, 0)
	defer a.Stop()
	var res []string
	a.OnReady(func(d []string) {
		res = d
	})
	a.Enqueue("one")
	a.Enqueue("two")
	a.Enqueue("three")
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, res)
	time.Sleep(150 * time.Millisecond)
	assert.ElementsMatch(t, res, []string{"one", "two", "three"})
	a.Enqueue("four")
	time.Sleep(210 * time.Millisecond)
	assert.ElementsMatch(t, res, []string{"four"})
}

func TestCapOverflow(t *testing.T) {
	a := NewAccumulator[string](200*time.Millisecond, 3)
	defer a.Stop()
	var res []string
	a.OnReady(func(d []string) {
		res = d
	})
	a.Enqueue("one")
	a.Enqueue("two")
	a.Enqueue("three")
	time.Sleep(100 * time.Millisecond)
	assert.Nil(t, res)
	a.Enqueue("four")
	time.Sleep(10 * time.Millisecond)
	assert.ElementsMatch(t, res, []string{"one", "two", "three"})
	time.Sleep(200 * time.Millisecond)
	assert.ElementsMatch(t, res, []string{"four"})
}
