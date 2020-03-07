/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: Defines the RoutingTable type and provides methods for interacting
 *  with it.
 */

package tapestry

import (
	"sort"
	"sync"
)

// RoutingTable has a number of levels equal to the number of digits in an ID
// (default 40). Each level has a number of slots equal to the digit base
// (default 16). A node that exists on level n thereby shares a prefix of length
// n with the local node. Access to the routing table protected by a mutex.
type RoutingTable struct {
	local RemoteNode                 // The local tapestry node
	rows  [DIGITS][BASE][]RemoteNode // The rows of the routing table
	mutex sync.Mutex                 // To manage concurrent access to the routing table (could also have a per-level mutex)
}

// NewRoutingTable creates and returns a new routing table, placing the local node at the
// appropriate slot in each level of the table.
func NewRoutingTable(me RemoteNode) *RoutingTable {
	t := new(RoutingTable)
	t.local = me

	// Create the node lists with capacity of SLOTSIZE
	for i := 0; i < DIGITS; i++ {
		for j := 0; j < BASE; j++ {
			t.rows[i][j] = make([]RemoteNode, 0, SLOTSIZE)
		}
	}

	// Make sure each row has at least our node in it
	for i := 0; i < DIGITS; i++ {
		slot := t.rows[i][t.local.ID[i]]
		t.rows[i][t.local.ID[i]] = append(slot, t.local)
	}

	return t
}

// Add adds the given node to the routing table.
//
// Returns true if the node did not previously exist in the table and was subsequently added.
// Returns the previous node in the table, if one was overwritten.
func (t *RoutingTable) Add(node RemoteNode) (added bool, previous *RemoteNode) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO: students should implement this
	level := SharedPrefixLength(node.ID, t.local.ID)
	if level == DIGITS {
		added = false
		return added, nil
	}
	slot := t.rows[level][node.ID[level]]

	slot = append(slot, node)
	sort.SliceStable(slot, func(i,j int) bool {
		return t.local.ID.Closer(slot[i].ID, slot[j].ID)
	})

	if len(slot) == 4 {
		*previous = slot[3]
		if slot[3] == node {
			added = false
		} else {
			added = true
		}
		t.rows[level][node.ID[level]] = slot[:3]
	}

	return added, previous
}

// Remove removes the specified node from the routing table, if it exists.
// Returns true if the node was in the table and was successfully removed.
func (t *RoutingTable) Remove(node RemoteNode) (wasRemoved bool) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO: students should implement this
	level := SharedPrefixLength(node.ID, t.local.ID)
	if level == DIGITS {
		wasRemoved = false
		return
	}
	slot := t.rows[level][node.ID[level]]
	for i:=0; i<len(slot); i++ {
		n := slot[i]
		if n == node {
			t.rows[level][node.ID[level]] = append(slot[:i], slot[i+1:]...)
			wasRemoved = true
			return
		}
	}
	wasRemoved = false
	return
}

// GetLevel get all nodes on the specified level of the routing table, EXCLUDING the local node.
func (t *RoutingTable) GetLevel(level int) (nodes []RemoteNode) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// TODO: students should implement this
	for i:=0; i<BASE; i++ {
		slot := t.rows[level][i]
		for j:=0; j<len(slot); j++ {
			n := slot[j]
			if n != t.local {
				nodes = append(nodes, n)
			}
		}
	}
	return
}

// FindNextHop searches the table for the closest next-hop node for the provided ID at the given level.
func (t *RoutingTable) FindNextHop(id ID, level int32) RemoteNode {
	// TODO: students should implement this
	if level >= DIGITS {
		return t.local
	}
	digit := id[level]
	slot := t.rows[level][digit]
	for {
		if len(slot) != 0 {
			closest := slot[0]
			for i:=1; i<len(slot); i++ {
				node := slot[i]
				if !id.Closer(closest.ID, node.ID) {
					closest = node
				}
			}
			return closest
		}
		digit += 1
		digit %= BASE
		slot = t.rows[level][digit]
	}
	// return RemoteNode{}
}






