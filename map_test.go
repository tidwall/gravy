// https://github.com/tidwall/gravy
//
// Copyright 2026 Joshua J Baker. All rights reserved.
package gravy

import (
	"math/rand/v2"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/tidwall/lotsa"
)

type geom struct {
	rect [4]float64
}

func randRect(rng *rand.Rand) *geom {
	x1 := rng.Float64()*360 - 180
	y1 := rng.Float64()*180 - 90
	size := 2.0
	deltaX := rng.Float64()*size - size/2
	deltaY := rng.Float64()*size - size/2
	x2 := x1 + deltaX
	y2 := y1 + deltaY
	r := [4]float64{min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2)}
	return &geom{r}
}

func randPoint(rng *rand.Rand) *geom {
	x1 := rng.Float64()*360 - 180
	y1 := rng.Float64()*180 - 90
	r := [4]float64{x1, y1, x1, y1}
	return &geom{r}
}

func TestSpatial(t *testing.T) {
	seed := uint64(time.Now().UnixNano())
	N := 1000000
	T := runtime.GOMAXPROCS(0)
	rngs := make([]*rand.Rand, T)
	for i := range T {
		rngs[i] = rand.New(rand.NewPCG(seed, uint64(i)))
	}
	geoms := make([]*geom, N)
	var spatial Map[*geom]
	spatial.validate = true
	lotsa.Output = os.Stdout
	print("insert ")
	lotsa.Ops(N, T, func(i, t int) {
		rng := rngs[t]
		var g *geom
		if i%2 == 0 {
			g = randRect(rng)
		} else {
			g = randPoint(rng)
		}
		geoms[i] = g
		tx := spatial.Begin(g.rect)
		tx.Insert(g.rect, g)
		tx.End()
	})
	spatial.Sane()
	println(spatial.Count())

	print("search ")
	lotsa.Ops(N, T, func(i, t int) {
		g := geoms[i]
		tx := spatial.Begin(g.rect)
		var found bool
		tx.Search(g.rect, func(r [4]float64, g2 *geom) bool {
			if g2 == g {
				found = true
				return false
			}
			return true
		})
		tx.End()
		if !found {
			panic("not found")
		}
	})
	spatial.Sane()
	println(spatial.Count())

	print("delete ")
	lotsa.Ops(N, T, func(i, t int) {
		g := geoms[i]
		tx := spatial.Begin(g.rect)
		tx.Delete(g.rect, g)
		tx.End()
	})

	spatial.Sane()
	println(spatial.Count())

}

func testPerf(N, T int) {
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(seed, 0))

	geoms := make([]*geom, N)
	for i := range geoms {
		if i%2 == 0 {
			geoms[i] = randRect(rng)
		} else {
			geoms[i] = randPoint(rng)
		}
	}
	var spatial Map[*geom]
	lotsa.Output = os.Stdout
	print("insert ")
	lotsa.Ops(N, T, func(i, t int) {
		g := geoms[i]
		tx := spatial.Begin(g.rect)
		tx.Insert(g.rect, g)
		tx.End()
	})

}

func TestPerf(t *testing.T) {
	N := 2500000
	for t := 1; t <= runtime.GOMAXPROCS(0); t++ {
		testPerf(N, t)
	}
}

func TestExample1(t *testing.T) {
	// Create a map
	var m Map[string]

	phx := [4]float64{-112.073, 33.448, -112.073, 33.448}

	// Store location of Phoenix
	tx := m.Begin(phx)
	tx.Insert(phx, "Phoenix")
	tx.End()

	// Search for items intersecting Phoenix
	tx = m.Begin(phx)
	tx.Search(phx, func(rect [4]float64, city string) bool {
		println(city)
		return true
	})
	tx.End()

	// Delete Phoenix
	tx = m.Begin(phx)
	tx.Delete(phx, "Phoenix")
	tx.End()

	// Output:
	// Phoenix
}

func TestExample2(t *testing.T) {
	// Create a map
	var m Map[string]

	phx := [4]float64{-112.073, 33.448, -112.073, 33.448}
	pra := [4]float64{14.420, 50.088, 14.420, 50.088}
	her := [4]float64{-110.977, 29.102, -110.977, 29.102}

	// Store locations
	tx := m.Begin(phx, pra, her)
	tx.Insert(phx, "Phoenix")
	tx.Insert(pra, "Prague")
	tx.Insert(her, "Hermosillo")
	tx.End()

	// Search for items intersecting a rectangle (sw-US nw-MX)
	rect := [4]float64{-120, 28, -110, 36}
	tx = m.Begin(rect)
	tx.Search(rect, func(rect [4]float64, city string) bool {
		println(city)
		return true
	})
	tx.End()

	// Delete location
	tx = m.Begin(phx, pra, her)
	tx.Delete(phx, "Phoenix")
	tx.Delete(pra, "Prague")
	tx.Delete(her, "Hermosillo")
	tx.End()

	// Output:
	// Phoenix
	// Hermosillo
}

func (b *branchNode[T]) sane(depth int, validate bool) {
	for i := range 4 {
		kind := b.states[i].kind.Load()
		if validate {
			if b.states[i].txid != 0 {
				panic("invalid state")
			}
			if !b.states[i].lock.TryLock() {
				panic("invalid state")
			}
			b.states[i].lock.Unlock()
		}
		switch kind {
		case kindBranch:
			branch := (*branchNode[T])(b.nodes[i])
			branch.sane(depth+1, validate)
		case kindLeaf:
			leaf := (*leafNode[T])(b.nodes[i])
			if leaf != nil {
				if len(leaf.items) == 0 {
					panic("invalid number of items")
				}
			}
		default:
			panic("invalid kind")
		}
	}
}

func (m *Map[T]) Sane() {
	m.root.sane(0, m.validate)
}

func (b *branchNode[T]) count() int {
	var count int
	for i := range 4 {
		if b.states[i].kind.Load() == kindBranch {
			count += (*branchNode[T])(b.nodes[i]).count()
		} else if b.nodes[i] != nil {
			leaf := (*leafNode[T])(b.nodes[i])
			b.states[i].lock.Lock()
			count += len(leaf.items)
			b.states[i].lock.Unlock()
		}
	}
	return count
}

func (s *Map[T]) Count() int {
	return s.root.count()
}
