/*
 *  Brown University, CS138, Spring 2020
 *
 *  Purpose: Defines functions for a node leaving the Tapestry mesh, and
 *  transferring its stored locations to a new node.
 */

package tapestry

import (
// Uncomment for xtrace
// xtr "github.com/brown-csci1380/tracing-framework-go/xtrace/client"
)

// Kill this node without gracefully leaving the tapestry.
func (local *Node) Kill() {
	local.blobstore.DeleteAll()
	local.server.Stop()
}

// Leave gracefully exits the Tapestry mesh.
//
// - Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
// - If possible, give each backpointer a suitable alternative node from our routing table
func (local *Node) Leave() (err error) {
	// TODO: students should implement this
	local.blobstore.DeleteAll()
	go local.server.GracefulStop()
	return
}

// NotifyLeave occurs when another node is informing us of a graceful exit.
// - Remove references to the `from` node from our routing table and backpointers
// - If replacement is not nil or `RemoteNode{}`, add replacement to our routing table
func (local *Node) NotifyLeave(from RemoteNode, replacement *RemoteNode) (err error) {
	Debug.Printf("Received leave notification from %v with replacement node %v\n", from, replacement)

	// TODO: students should implement this
	return
}
