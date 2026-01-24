# gravy

Vertically scalable spatial map for rapidly changing location data.

- Designed for high concurrency and low contention
- Uses a 2PL type locking mechanism
- Works with rectangles, similar to an rtree
- Uses a quadtree type structure under the hood
- The `Search` operation may return duplicates
- The input rectangle for the `Insert`, `Search` and `Delete` operations must be covered by the previous `Begin`, otherwise returns `ErrNotCovered`
- A transaction may cover multiple rectangles by calling `Begin(rect1, rect2, rect3, ...)`

## Example

Insert, search, and delete.

```go

// Create a map
var m gravy.Map[string]

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
```

Multiple rectangles

```go
// Create a map
var m gravy.Map[string]

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
```
