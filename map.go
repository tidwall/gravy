// https://github.com/tidwall/gravy
//
// Copyright 2026 Joshua J Baker. All rights reserved.
package gravy

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

const maxItems = 64
const maxDepth = 32

var (
	ErrNotCovered = errors.New("rectangle not covered")
	ErrTxEnded    = errors.New("transaction ended")
)

const (
	// first 2 bits are the node kind
	kindLeaf      = 0
	kindBranch    = 1
	kindLeafSplit = 2
	// copy on write flags
	kindCloned       = 8  // bit 3
	kindClonedLocked = 16 // bit 4
)

type point struct {
	x, y float64
}

type rect struct {
	min, max point
}

var bounds = rect{point{-180, -90}, point{180, 90}}

type Tx[T comparable] struct {
	id          uint64
	withonerect bool
	onerect     rect
	m           *Map[T]
	ended       bool
	rects0      []rect
}

type state[T comparable] struct {
	kind atomic.Int32
	txid uint64
	lock sync.Mutex
}

type branchNode[T comparable] struct {
	states [4]state[T]
	nodes  [4]unsafe.Pointer // either *sleaf[T] or *sbranch[T]
}

type item[T comparable] struct {
	rect rect
	data T
}

type leafNode[T comparable] struct {
	items []item[T]
}

type oobNode[T comparable] struct {
	txid  uint64
	mutex sync.Mutex
	items []item[T]
}

type Map[T comparable] struct {
	validate bool
	root     branchNode[T]
	oob      oobNode[T] // out of bounds items
}

func rectContains(a, b rect) bool {
	if b.min.x < a.min.x || b.max.x > a.max.x {
		return false
	}
	if b.min.y < a.min.y || b.max.y > a.max.y {
		return false
	}
	return true
}

func rectIntersects(a, b rect) bool {
	if b.min.x > a.max.x || b.max.x < a.min.x {
		return false
	}
	if b.min.y > a.max.y || b.max.y < a.min.y {
		return false
	}
	return true
}

func calcQuads(r rect) [4]rect {
	mid := point{(r.min.x + r.max.x) / 2, (r.min.y + r.max.y) / 2}
	return [4]rect{
		{point{r.min.x, mid.y}, point{mid.x, r.max.y}},
		{point{mid.x, mid.y}, point{r.max.x, r.max.y}},
		{point{r.min.x, r.min.y}, point{mid.x, mid.y}},
		{point{mid.x, r.min.y}, point{r.max.x, mid.y}},
	}
}

func (n *oobNode[T]) lock(bounds rect, tx *Tx[T], validate bool) {
	var ok bool
	if tx.withonerect {
		if !rectContains(bounds, tx.onerect) {
			ok = true
		}
	} else {
		for i := range tx.rects0 {
			if !rectContains(bounds, tx.rects0[i]) {
				ok = true
				break
			}
		}
	}
	if ok {
		n.mutex.Lock()
		if validate {
			if tx.m.oob.txid != 0 {
				panic("invalid state")
			}
			tx.m.oob.txid = tx.id
		}
	}
}

func (n *oobNode[T]) unlock(bounds rect, tx *Tx[T], validate bool) {
	var ok bool
	if tx.withonerect {
		if !rectContains(bounds, tx.onerect) {
			ok = true
		}
	} else {
		for i := range tx.rects0 {
			if !rectContains(bounds, tx.rects0[i]) {
				ok = true
				break
			}
		}
	}
	if ok {
		if validate {
			if tx.m.oob.txid != tx.id {
				panic("invalid state")
			}
			tx.m.oob.txid = 0
		}
		n.mutex.Unlock()
	}
}

func (n *oobNode[T]) delete(bounds rect, item item[T], txid uint64,
	validate bool,
) {
	if rectContains(bounds, item.rect) {
		return
	}
	if validate {
		if n.mutex.TryLock() || n.txid != txid {
			panic("invalid state")
		}
	}
	for i := range len(n.items) {
		if n.items[i].data == item.data {
			var empty T
			n.items[i] = n.items[len(n.items)-1]
			n.items[len(n.items)-1].data = empty
			n.items = n.items[:len(n.items)-1]
			break
		}
	}
}

func (n *oobNode[T]) insert(bounds rect, item item[T], txid uint64,
	validate bool,
) {
	if rectContains(bounds, item.rect) {
		return
	}
	if validate {
		if n.mutex.TryLock() || n.txid != txid {
			panic("invalid state")
		}
	}
	n.items = append(n.items, item)
}

func (n *oobNode[T]) search(bounds rect, rect rect,
	iter func(rect [4]float64, data T) bool, txid uint64, validate bool,
) bool {
	if rectContains(bounds, rect) {
		return true
	}
	if validate {
		if n.mutex.TryLock() || n.txid != txid {
			panic("invalid state")
		}
	}
	for _, item := range n.items {
		if !rectIntersects(rect, item.rect) {
			continue
		}
		r := [4]float64{
			item.rect.min.x, item.rect.min.y,
			item.rect.max.x, item.rect.max.y,
		}
		if !iter(r, item.data) {
			return false
		}
	}
	return true
}

func (b *branchNode[T]) cow(i int, validate bool) {
	// clone bit flag set
	kind := b.states[i].kind.Load()
	if kind < 4 {
		return
	}
	if kind&kindClonedLocked == kindClonedLocked {
		// Already in the process of being cloned
		return
	}
	if validate {
		if kind&kindCloned != kindCloned {
			panic("invalid state")
		}
	}
	if !b.states[i].kind.CompareAndSwap(kind, kind|kindClonedLocked) {
		return
	}
	kind = kind & 3
	if kind == kindBranch {
		b1 := (*branchNode[T])(b.nodes[i])
		b2 := new(branchNode[T])
		b.nodes[i] = unsafe.Pointer(b2)
		for i := range b1.nodes {
			kind := b1.states[i].kind.Load()
			b2.states[i].kind.Store(kind | kindCloned)
			b2.nodes[i] = b1.nodes[i]
		}
	} else {
		l1 := (*leafNode[T])(b.nodes[i])
		if l1 != nil {
			l2 := new(leafNode[T])
			l2.items = append(l2.items, l1.items...)
			b.nodes[i] = unsafe.Pointer(l2)
		}
	}
	b.states[i].kind.Store(kind)
}

func (b *branchNode[T]) lock(bounds rect, tx *Tx[T], validate bool) {
	quads := calcQuads(bounds)
	for i := range 4 {
		var ok bool
		if tx.withonerect {
			if rectIntersects(tx.onerect, quads[i]) {
				ok = true
			}
		} else {
			for j := range tx.rects0 {
				if rectIntersects(tx.rects0[j], quads[i]) {
					ok = true
					break
				}
			}
		}
		if !ok {
			continue
		}
		for {
			kind := b.states[i].kind.Load()
			if kind >= 4 {
				b.cow(int(i), validate)
				runtime.Gosched()
				continue
			}
			if kind == kindBranch {
				(*branchNode[T])(b.nodes[i]).lock(quads[i], tx, validate)
				break
			}
			b.states[i].lock.Lock()
			kind = b.states[i].kind.Load()
			if kind == kindLeaf {
				if validate {
					if b.states[i].txid != 0 {
						panic("invalid state")
					}
					b.states[i].txid = tx.id
				}
				break
			}
			if validate {
				if kind == kindLeafSplit {
					panic("invalid state")
				}
			}
			b.states[i].lock.Unlock()
		}
	}
}

func (b *branchNode[T]) unlock(bounds rect, tx *Tx[T], validate bool) {
	quads := calcQuads(bounds)
	for i := range 4 {
		var ok bool
		if tx.withonerect {
			if rectIntersects(tx.onerect, quads[i]) {
				ok = true
			}
		} else {
			for j := range len(tx.rects0) {
				if rectIntersects(tx.rects0[j], quads[i]) {
					ok = true
					break
				}
			}
		}
		if !ok {
			continue
		}
		kind := b.states[i].kind.Load()
		if kind == kindBranch {
			(*branchNode[T])(b.nodes[i]).unlock(quads[i], tx, validate)
			continue
		}
		if validate {
			if b.states[i].txid != tx.id {
				panic("invalid state")
			}
		}
		if kind == kindLeafSplit {
			// Leaf was converted to branch due to a split.
			// Switch to a branch before unlocking
			b.states[i].kind.Store(kindBranch)
		}
		if validate {
			b.states[i].txid = 0
		}
		b.states[i].lock.Unlock()
	}
}

func (s *Map[T]) insertAfterSplit(b *branchNode[T], bounds rect,
	leaf *leafNode[T],
) {
	quads := calcQuads(bounds)
	for i := range 4 {
		for j := 0; j < len(leaf.items); j++ {
			item := leaf.items[j]
			if !rectIntersects(item.rect, quads[i]) {
				continue
			}
			leaf := (*leafNode[T])(b.nodes[i])
			if leaf == nil {
				leaf = new(leafNode[T])
				b.nodes[i] = unsafe.Pointer(leaf)
			}
			leaf.items = append(leaf.items, item)
		}
	}
}

func (s *Map[T]) insert(b *branchNode[T], bounds rect, item item[T],
	depth int, split bool, txid uint64, validate bool,
) {
	quads := calcQuads(bounds)
	for i := range 4 {
		if !rectIntersects(item.rect, quads[i]) {
			continue
		}
		kind := b.states[i].kind.Load()
		if kind == kindBranch || kind == kindLeafSplit {
			var split2 bool
			if kind == kindLeafSplit {
				if validate {
					if b.states[i].txid != txid {
						panic("invalid state")
					}
					if b.states[i].lock.TryLock() {
						panic("invalid state")
					}
				}
				split2 = true
			}
			child := (*branchNode[T])(b.nodes[i])
			s.insert(child, quads[i], item, depth+1, split2, txid, validate)
		} else {
			if validate {
				if b.states[i].txid != txid {
					panic("invalid state")
				}
				if b.states[i].lock.TryLock() {
					panic("invalid state")
				}
			}
			leaf := (*leafNode[T])(b.nodes[i])
			if leaf == nil {
				leaf = new(leafNode[T])
				b.nodes[i] = unsafe.Pointer(leaf)
			}
			leaf.items = append(leaf.items, item)
			if !split && len(leaf.items) >= maxItems && depth < maxDepth {
				// Split leaf. Convert to branch
				branch2 := new(branchNode[T])
				b.states[i].kind.Store(kindLeafSplit)
				b.nodes[i] = unsafe.Pointer(branch2)
				s.insertAfterSplit(branch2, quads[i], leaf)
			}
		}
	}
}

func (s *Map[T]) delete(b *branchNode[T], bounds rect, item item[T],
	txid uint64, validate bool,
) {
	var empty T
	quads := calcQuads(bounds)
	for i := range 4 {
		if !rectIntersects(item.rect, quads[i]) {
			continue
		}
		kind := b.states[i].kind.Load()
		if kind == kindBranch || kind == kindLeafSplit {
			if kind == kindLeafSplit {
				if validate {
					if b.states[i].txid != txid {
						panic("invalid state")
					}
					if b.states[i].lock.TryLock() {
						panic("invalid state")
					}
				}
			}
			child := (*branchNode[T])(b.nodes[i])
			s.delete(child, quads[i], item, txid, validate)
		} else {
			if validate {
				if b.states[i].txid != txid {
					panic("invalid state")
				}
				if b.states[i].lock.TryLock() {
					panic("invalid state")
				}
			}
			leaf := (*leafNode[T])(b.nodes[i])
			if leaf == nil {
				continue
			}
			for j := range leaf.items {
				if leaf.items[j].data == item.data {
					leaf.items[j] = leaf.items[len(leaf.items)-1]
					leaf.items[len(leaf.items)-1].data = empty
					leaf.items = leaf.items[:len(leaf.items)-1]
					break
				}
			}
			if len(leaf.items) == 0 {
				b.nodes[i] = nil
			}
		}
	}
}

func (b *branchNode[T]) search(bounds, rect rect,
	iter func(rect [4]float64, data T) bool, txid uint64, validate bool,
) bool {
	quads := calcQuads(bounds)
	for i := range 4 {
		if !rectIntersects(rect, quads[i]) {
			continue
		}
		kind := b.states[i].kind.Load()
		if kind == kindBranch || kind == kindLeafSplit {
			if kind == kindLeafSplit {
				if validate {
					if b.states[i].txid != txid {
						panic("invalid state")
					}
					if b.states[i].lock.TryLock() {
						panic("invalid state")
					}
				}
			}
			child := (*branchNode[T])(b.nodes[i])
			if !child.search(quads[i], rect, iter, txid, validate) {
				return false
			}
		} else {
			if validate {
				if b.states[i].txid != txid {
					panic("invalid state")
				}
				if b.states[i].lock.TryLock() {
					panic("invalid state")
				}
			}
			leaf := (*leafNode[T])(b.nodes[i])
			if leaf == nil {
				continue
			}
			for _, item := range leaf.items {
				if !rectIntersects(item.rect, rect) {
					continue
				}
				r := [4]float64{
					item.rect.min.x, item.rect.min.y,
					item.rect.max.x, item.rect.max.y,
				}
				if !iter(r, item.data) {
					return false
				}
			}
		}
	}
	return true
}

func rect4(r [4]float64) rect {
	return rect{point{r[0], r[1]}, point{r[2], r[3]}}
}

var txidc atomic.Uint64

func (m *Map[T]) Begin(rects ...[4]float64) Tx[T] {
	tx := Tx[T]{m: m}
	if m.validate {
		tx.id = txidc.Add(1)
	}
	if len(rects) == 1 {
		tx.withonerect = true
		tx.onerect = rect4(rects[0])
	} else {
		tx.rects0 = *(*[]rect)(unsafe.Pointer(&rects))
	}
	tx.m.oob.lock(bounds, &tx, m.validate)
	tx.m.root.lock(bounds, &tx, m.validate)
	return tx
}

func (tx *Tx[T]) End() error {
	if tx.ended {
		return ErrTxEnded
	}
	tx.m.oob.unlock(bounds, tx, tx.m.validate)
	tx.m.root.unlock(bounds, tx, tx.m.validate)
	tx.ended = true
	return nil
}

func (tx *Tx[T]) validate(rect rect) error {
	if tx.ended {
		return ErrTxEnded
	}
	var ok bool
	if tx.withonerect {
		if rectContains(tx.onerect, rect) {
			ok = true
		}
	} else {
		for i := range tx.rects0 {
			if rectContains(tx.rects0[i], rect) {
				ok = true
				break
			}
		}
	}
	if !ok {
		return ErrNotCovered
	}
	return nil
}

func (tx *Tx[T]) Insert(rect [4]float64, data T) error {
	item := item[T]{rect4(rect), data}
	if err := tx.validate(item.rect); err != nil {
		return err
	}
	tx.m.oob.insert(bounds, item, tx.id, tx.m.validate)
	tx.m.insert(&tx.m.root, bounds, item, 0, false, tx.id, tx.m.validate)
	return nil
}

func (tx *Tx[T]) Delete(rect [4]float64, data T) error {
	item := item[T]{rect4(rect), data}
	if err := tx.validate(item.rect); err != nil {
		return err
	}
	tx.m.oob.delete(bounds, item, tx.id, tx.m.validate)
	tx.m.delete(&tx.m.root, bounds, item, tx.id, tx.m.validate)
	return nil
}

func (tx *Tx[T]) Search(rect [4]float64,
	iter func(rect [4]float64, data T) bool,
) error {
	r := rect4(rect)
	if err := tx.validate(r); err != nil {
		return err
	}
	if tx.m.oob.search(bounds, r, iter, tx.id, tx.m.validate) {
		tx.m.root.search(bounds, r, iter, tx.id, tx.m.validate)
	}
	return nil
}

// Clone of the map.
// This is an O(1) Copy-on-write.
// WARNING: This operation requires exclusive access to the map. Do not call
// while other transactions are sharing the same map.
// It's your responsibility to manage access using a lock, such as with a
// sync.RWLock.
func (m *Map[T]) Clone() *Map[T] {
	m2 := new(Map[T])
	for i := range m.root.nodes {
		kind := m.root.states[i].kind.Load()
		m.root.states[i].kind.Store(kind | kindCloned)
		m2.root.states[i].kind.Store(kind | kindCloned)
		m2.root.nodes[i] = m.root.nodes[i]
	}
	if len(m.oob.items) > 0 {
		m2.oob.items = append(m2.oob.items, m.oob.items...)
	}
	return m2
}

func (b *branchNode[T]) scan(iter func(rect [4]float64, data T) bool,
	validate bool,
) bool {
	for i := range b.nodes {
		kind := b.states[i].kind.Load() & 3
		if kind == kindBranch {
			if !(*branchNode[T])(b.nodes[i]).scan(iter, validate) {
				return false
			}
		} else {
			if validate {
				if kind != kindLeaf {
					panic("invalid state")
				}
			}
			leaf := (*leafNode[T])(b.nodes[i])
			if leaf != nil {
				for _, item := range leaf.items {
					r := [4]float64{
						item.rect.min.x, item.rect.min.y,
						item.rect.max.x, item.rect.max.y,
					}
					if !iter(r, item.data) {
						return false
					}
				}
			}
		}
	}
	return true
}

// Scan the map, iterating over all keys and values.
// WARNING: This operation requires exclusive access to the map. Do not call
// while other transactions are sharing the same map.
// It's your responsibility to manage access using a lock, such as with a
// sync.RWLock.
func (m *Map[T]) Scan(iter func(rect [4]float64, data T) bool) {
	if m.root.scan(iter, m.validate) {
		for _, item := range m.oob.items {
			r := [4]float64{
				item.rect.min.x, item.rect.min.y,
				item.rect.max.x, item.rect.max.y,
			}
			if !iter(r, item.data) {
				break
			}
		}
	}
}
