package main

import "fmt"

func main() {
	dal, _ := NewDataAccessLayer("./mainTest")

	node, _ := dal.getNode(dal.Meta.Root)
	node.DALayer = dal

	index, containingNode, _ := node.findKey([]byte("Key1"))
	res := containingNode.items[index]

	fmt.Printf("key is: %s, value is: %s", res.key, res.val)
	// close the db
	_ = dal.Close()
}
