/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: Defines global constants and functions to create and join a new
 *  node into a Tapestry mesh, and functions for altering the routing table
 *  and backpointers of the local node that are invoked over RPC.
 */

package tapestry

import (
	"fmt"
	"net"
	"os"
	"time"
	"sort"
	// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
	// util "github.com/brown-csci1380/tracing-framework-go/xtrace/grpcutil"
	"google.golang.org/grpc"
)

// BASE is the base of a digit of an ID.  By default, a digit is base-16.
const BASE = 16

// DIGITS is the number of digits in an ID.  By default, an ID has 40 digits.
const DIGITS = 40

// RETRIES is the number of retries on failure. By default we have 3 retries.
const RETRIES = 3

// K is neigborset size during neighbor traversal before fetching backpointers. By default this has a value of 10.
const K = 10

// SLOTSIZE is the size each slot in the routing table should store this many nodes. By default this is 3.
const SLOTSIZE = 3

// REPUBLISH is object republish interval for nodes advertising objects.
const REPUBLISH = 10 * time.Second

// TIMEOUT is object timeout interval for nodes storing objects.
const TIMEOUT = 25 * time.Second

// Node is the main struct for the local Tapestry node. Methods can be invoked locally on this struct.
type Node struct {
	node           RemoteNode    // The ID and address of this node
	table          *RoutingTable // The routing table
	backpointers   *Backpointers // Backpointers to keep track of other nodes that point to us
	locationsByKey *LocationMap  // Stores keys for which this node is the root
	blobstore      *BlobStore    // Stores blobs on the local node
	server         *grpc.Server
}

func (local *Node) String() string {
	return fmt.Sprintf("Tapestry Node %v at %v", local.node.ID, local.node.Address)
}

// Called in tapestry initialization to create a tapestry node struct
func newTapestryNode(node RemoteNode) *Node {
	serverOptions := []grpc.ServerOption{}
	// Uncomment for xtrace
	// serverOptions = append(serverOptions, grpc.UnaryInterceptor(util.XTraceServerInterceptor))
	n := new(Node)

	n.node = node
	n.table = NewRoutingTable(node)
	n.backpointers = NewBackpointers(node)
	n.locationsByKey = NewLocationMap()
	n.blobstore = NewBlobStore()
	n.server = grpc.NewServer(serverOptions...)

	return n
}

// Start a tapestry node on the specified port. Optionally, specify the address
// of an existing node in the tapestry mesh to connect to; otherwise set to "".
func Start(port int, connectTo string) (*Node, error) {
	return start(RandomID(), port, connectTo)
}

// Private method, useful for testing: start a node with the specified ID rather than a random ID.
func start(id ID, port int, connectTo string) (tapestry *Node, err error) {

	// Create the RPC server
	lis, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	if err != nil {
		// fmt.Println("1")
		return nil, err
	}

	// Get the hostname of this machine
	name, err := os.Hostname()
	if err != nil {
		// fmt.Println("2")
		return nil, fmt.Errorf("Unable to get hostname of local machine to start Tapestry node. Reason: %v", err)
	}

	// Get the port we are bound to
	_, actualport, err := net.SplitHostPort(lis.Addr().String()) //fmt.Sprintf("%v:%v", name, port)
	if err != nil {
		// fmt.Println("3")
		return nil, err
	}

	// The actual address of this node
	address := fmt.Sprintf("%s:%s", name, actualport)

	// Uncomment for xtrace
	// xtr.NewTask("startup")
	// Trace.Print("Tapestry Starting up...")
	// xtr.SetProcessName(fmt.Sprintf("Tapestry %X... (%v)", id[:5], address))

	// Create the local node
	tapestry = newTapestryNode(RemoteNode{ID: id, Address: address})
	fmt.Printf("Created tapestry node %v\n", tapestry)
	Trace.Printf("Created tapestry node")

	RegisterTapestryRPCServer(tapestry.server, tapestry)
	fmt.Printf("Registered RPC Server\n")
	go tapestry.server.Serve(lis)

	// If specified, connect to the provided address
	if connectTo != "" {
		// Get the node we're joining
		node, err := SayHelloRPC(connectTo, tapestry.node)
		if err != nil {
			// fmt.Println("4")
			return nil, fmt.Errorf("Error joining existing tapestry node %v, reason: %v", address, err)
		}
		err = tapestry.Join(node)
		if err != nil {
			// fmt.Println("5")
			return nil, err
		}
	}

	return tapestry, nil
}

// Join is invoked when starting the local node, if we are connecting to an existing Tapestry.
//
// - Find the root for our node's ID
// - Call AddNode on our root to initiate the multicast and receive our initial neighbor set. Add them to our table.
// - Iteratively get backpointers from the neighbor set for all levels in range [0, SharedPrefixLength]
// - 	and populate routing table
func (local *Node) Join(otherNode RemoteNode) (err error) {
	Debug.Println("Joining", otherNode)

	// Route to our root
	root, err := local.findRootOnRemoteNode(otherNode, local.node.ID)
	if err != nil {
		// fmt.Println("01")
		return fmt.Errorf("Error joining existing tapestry node %v, reason: %v", otherNode, err)
	}
	// Add ourselves to our root by invoking AddNode on the remote node
	neighbors, err := root.AddNodeRPC(local.node)
	if err != nil {
		// fmt.Println("02")
		fmt.Printf("Error adding ourselves to root node %v, reason: %v\n", root, err)
		return fmt.Errorf("Error adding ourselves to root node %v, reason: %v", root, err)
	}

	// Add the neighbors to our local routing table.
	for _, n := range neighbors {
		local.addRoute(n)
	}

	// TODO: students should implement the backpointer traversal portion of Join
	//Populate rest of joining Node Routing Table by getting backpointers of neighbor set
	err = local.TraverseBackpointers(neighbors,SharedPrefixLength(otherNode.ID, local.node.ID))
	if err != nil {
		// fmt.Println("03")
	}
	return err
}

func (local *Node) TraverseBackpointers(neighbors []RemoteNode, level int) (err error){
	for level >= 0 {
		sort.SliceStable(neighbors, func(i,j int) bool {
			return local.node.ID.Closer(neighbors[i].ID, neighbors[j].ID)
		})
		if len(neighbors) > K{
			neighbors = neighbors[:K]
		}
		tmp := make([]RemoteNode, 0)
		for _, n := range neighbors {
			bps, err := n.GetBackpointersRPC(local.node, level)
			if err != nil {
				return err
			}
			for _, bp := range bps {
				tmp = append(tmp, bp)
			}
		}
		tmp = local.Merge(tmp, make([]RemoteNode, 0))
		for _, n := range tmp {
			local.addRoute(n)
		}
		neighbors = local.Merge(neighbors, tmp)

		level -= 1
	}

	// for level >= 0{
	// 	nextNeighbors := neighbors
	// 	for _,n := range neighbors{
	// 		bps, err := local.node.GetBackpointersRPC(n, level)
	// 		if err != nil{
	// 			return err
	// 		}
	// 		for _,bp := range bps{
	// 			nextNeighbors =  append(nextNeighbors, bp)
	// 		}
	// 	}

	// 	// TODO: trim the nextNeighbors
	// 	nextNeighbors = local.Merge(nextNeighbors, make([]RemoteNode, 0))
	// 	for _,n := range nextNeighbors{
	// 		local.table.Add(n)
	// 	}
	// 	neighbors = nextNeighbors
	// 	if len(neighbors) > K{
	// 		neighbors = neighbors[:K]
	// 	}
	// 	level -= 1
	// }
	
	return err
}



// AddNode adds node to the tapestry
//
// - Begin the acknowledged multicast
// - Return the neighborset from the multicast
func (local *Node) AddNode(node RemoteNode) (neighborset []RemoteNode, err error) {
	return local.AddNodeMulticast(node, SharedPrefixLength(node.ID, local.node.ID))
}

// AddNodeMulticast sends AddNode to need-to-know nodes participating in the multicast.
//
// - Add the route for the new node (use `local.addRoute`)
// - Transfer of appropriate replica info to the new node. If error, rollback the location map.
// 		(use `local.locationsByKey.GetTransferRegistrations`)
// - Propagate the multicast to the specified row in our routing table and await multicast responses
// - Return the merged neighbor set
//
// - note: `local.table.GetLevel` does not return the local node

func (local *Node) AddNodeMulticast(newNode RemoteNode, level int) (neighbors []RemoteNode, err error) {
	Debug.Printf("Add node multicast %v at level %v\n", newNode, level)
	// TODO: students should implement this
	if level < DIGITS {
		err = local.addRoute(newNode)
		if err != nil {
			// fmt.Println("init 1")
			return nil, err
		}
		
		targets := local.table.GetLevel(level)
		for _, target := range targets {
			var tempNodes []RemoteNode
			tempNodes, err = target.AddNodeMulticastRPC(newNode, level+1)
			neighbors = local.Merge(neighbors, tempNodes)
		}
		nextNeighbors, _ := local.AddNodeMulticast(newNode, level+1)
		neighbors = local.Merge(neighbors, nextNeighbors)

		err = local.addRoute(newNode)

		go local.TransferRelevantObject(newNode)

		if err != nil {
			// fmt.Println("init 1")
			return nil, err
		}
		return neighbors, nil

	} else {
		return []RemoteNode{local.node}, nil
	}
}

func (local *Node) Merge(nodes1 []RemoteNode, nodes2 []RemoteNode) (rnt []RemoteNode){
	s := make(map[string]bool)
	for _, n := range nodes1{
		add, id := n.Address, n.ID
		k := add + id.String()
		val, found := s[k]
		if !found || val == false{
			s[k] = true
			rnt = append(rnt, n)
		}
	}
	for _, n := range nodes2{
		add, id := n.Address, n.ID
		k := add + id.String()
		val, found := s[k]
		if !found || val == false{
			s[k] = true
			rnt = append(rnt, n)
		}
	}
	return rnt
}

func (local *Node) TransferRelevantObject(newNode RemoteNode) (err error){
	m := local.locationsByKey.GetTransferRegistrations(local.node, newNode)
	err = newNode.TransferRPC(local.node, m)
	if err != nil{
		//roll back location map
		// fmt.Println("init 3")
		Debug.Printf("Error in transfer, roll back locationmap")
		local.locationsByKey.RegisterAll(m, TIMEOUT)
	}
	return err
}




// AddBackpointer adds the from node to our backpointers, and possibly add the node to our
// routing table, if appropriate
func (local *Node) AddBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Add(from) {
		Debug.Printf("Added backpointer %v\n", from)
	}
	local.addRoute(from)
	return
}

// RemoveBackpointer removes the from node from our backpointers
func (local *Node) RemoveBackpointer(from RemoteNode) (err error) {
	if local.backpointers.Remove(from) {
		Debug.Printf("Removed backpointer %v\n", from)
	}
	return
}

// GetBackpointers gets all backpointers at the level specified, and possibly add the node to our
// routing table, if appropriate
func (local *Node) GetBackpointers(from RemoteNode, level int) (backpointers []RemoteNode, err error) {
	Debug.Printf("Sending level %v backpointers to %v\n", level, from)
	backpointers = local.backpointers.Get(level)
	local.addRoute(from)
	return
}

// RemoveBadNodes discards all the provided nodes
// - Remove each node from our routing table
// - Remove each node from our set of backpointers
func (local *Node) RemoveBadNodes(badnodes []RemoteNode) (err error) {
	for _, badnode := range badnodes {
		if local.table.Remove(badnode) {
			Debug.Printf("Removed bad node %v\n", badnode)
		}
		if local.backpointers.Remove(badnode) {
			Debug.Printf("Removed bad node backpointer %v\n", badnode)
		}
	}
	return
}

// Utility function that adds a node to our routing table.
//
// - Adds the provided node to the routing table, if appropriate.
// - If the node was added to the routing table, notify the node of a backpointer
// - If an old node was removed from the routing table, notify the old node of a removed backpointer
func (local *Node) addRoute(node RemoteNode) (err error) {
	// TODO: students should implement this
	err = nil
	added, prevNode := local.table.Add(node)
	if added {
		//notify the node of a backpointer
		err = node.AddBackpointerRPC(local.node)
		if err != nil {
			//TODO: still need to think about what to do to add the backpointer
			local.RemoveBadNodes([]RemoteNode{node})
			if prevNode != nil {
				err := local.addRoute(*prevNode)
				if err == nil {
					Debug.Printf("Add back the evicted node  %v\n", prevNode)
				} else {
					Debug.Printf("Error in add back the evicted node  %v\n", prevNode)
				}
			}
		} else if prevNode != nil {
			err = prevNode.RemoveBackpointerRPC(local.node)
			if err != nil {
				Debug.Printf("Fail to remove backpointer of previous node %v\n", prevNode)
			}
		}
	}

	return err
}
