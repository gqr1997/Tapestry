/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: Defines IDs for tapestry and provides various utility functions
 *  for manipulating and creating them. Provides functions to compare IDs
 *  for insertion into routing tables, and for implementing the routing
 *  algorithm.
 */

package tapestry

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	"time"
)

// An ID is a digit array
// 40
type ID [DIGITS]Digit

// Digit is just a typedef'ed uint8
type Digit uint8

// Random number generator for generating random node ID
var random = rand.New(rand.NewSource(time.Now().UTC().UnixNano()))

// RandomID returns a random ID.
func RandomID() ID {
	var id ID
	for i := range id {
		id[i] = Digit(random.Intn(BASE))
	}
	return id
}

// Hash hashes the string to an ID
func Hash(key string) (id ID) {
	// Sha-hash the key
	sha := sha1.New()
	sha.Write([]byte(key))
	hash := sha.Sum([]byte{})

	// Store in an ID
	for i := range id {
		id[i] = Digit(hash[(i/2)%len(hash)])
		if i%2 == 0 {
			id[i] >>= 4
		}
		id[i] %= BASE
	}

	return id
}

// SharedPrefixLength returns the length of the prefix that is shared by the two IDs.
func SharedPrefixLength(a ID, b ID) (i int) {
	// TODO: students should implement this function
	i = 0
	for ix:=0; ix<DIGITS; ix++ {
		if a[ix]!=b[ix] {
			break
		}
		i += 1
	}
	return
}

// Used by Tapestry's surrogate routing.  Given IDs first and second, which is the better choice?
//
// The "better choice" is the ID that:
//  - has the longest shared prefix with id
//  - if both have prefix of length n, which id has a better (n+1)th digit?
//  - if both have the same (n+1)th digit, consider (n+2)th digit, etc.

// BetterChoice returns true if the first ID is the better choice.
// Returns false if second ID is closer or if first == second.
func (id ID) BetterChoice(first ID, second ID) bool {
	// TODO: students should implement this
	prefix_length1 := SharedPrefixLength(id, first)
	prefix_length2 := SharedPrefixLength(id, second)
	prefix_length3 := SharedPrefixLength(first, second)
	if prefix_length1 > prefix_length2 {
		return true
	} else if prefix_length1 == prefix_length2 && prefix_length3 < DIGITS {
		n1 := first[prefix_length3]
		n2 := second[prefix_length3]
		if n1 < id[prefix_length3] {
			n1 += 16
		}
		if n2 < id[prefix_length3] {
			n2 += 16
		}
		if n1 < n2 {
			return true
		}
	}

	return false
}

// Closer is used when inserting nodes into Tapestry's routing table.  If the routing
// table has multiple candidate nodes for a slot, then it chooses the node that
// is closer to the local node.
//
// In a production Tapestry implementation, closeness is determined by looking
// at the round-trip-times (RTTs) between (a, id) and (b, id); the node with the
// shorter RTT is closer.
//
// In this implementation, we have decided to define closeness as the absolute
// value of the difference between a and b. This is NOT the same as your
// implementation of BetterChoice.
//
// Return true if a is closer than b.
// Return false if b is closer than a, or if a == b.
func (id ID) Closer(first ID, second ID) bool {
	// TODO: students should implement this
	dis1 := first.big().Abs(first.big().Sub(first.big(), id.big()))
	dis2 := second.big().Abs(second.big().Sub(second.big(), id.big()))
	diff := dis1.Sub(dis1, dis2)
	if diff.Sign() == -1 {
		return true
	}
	return false
}

// Helper function: convert an ID to a big int.
func (id ID) big() (b *big.Int) {
	b = big.NewInt(0)
	base := big.NewInt(BASE)
	for _, digit := range id {
		b.Mul(b, base)
		b.Add(b, big.NewInt(int64(digit)))
	}
	return b
}

// String representation of an ID is hexstring of each digit.
func (id ID) String() string {
	var buf bytes.Buffer
	for _, d := range id {
		buf.WriteString(d.String())
	}
	return buf.String()
}

// String representation of a digit is its hex value
func (digit Digit) String() string {
	return fmt.Sprintf("%X", byte(digit))
}

func (id ID) bytes() []byte {
	b := make([]byte, len(id))
	for idx, d := range id {
		b[idx] = byte(d)
	}
	return b
}

func idFromBytes(b []byte) (i ID) {
	if len(b) < DIGITS {
		return
	}
	for idx, d := range b[:DIGITS] {
		i[idx] = Digit(d)
	}
	return
}

// ParseID parses an ID from String
func ParseID(stringID string) (ID, error) {
	var id ID

	if len(stringID) != DIGITS {
		return id, fmt.Errorf("Cannot parse %v as ID, requires length %v, actual length %v", stringID, DIGITS, len(stringID))
	}

	for i := 0; i < DIGITS; i++ {
		d, err := strconv.ParseInt(stringID[i:i+1], 16, 0)
		if err != nil {
			return id, err
		}
		id[i] = Digit(d)
	}

	return id, nil
}
