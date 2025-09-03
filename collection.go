package main

import (
	"bytes"
	"fmt"
)

type Collection struct {
	name []byte
	root pagenum

	dal *DALayer
}

func NewCollection(name []byte, root pagenum) *Collection {
	return &Collection{
		name: name,
		root: root,
	}
}

// Find returns an item based on the given key using binary search
func (c *Collection) Find(key []byte) (*Item, error) {
	n, err := c.dal.getNode(c.root)
	if err != nil {
		return nil, fmt.Errorf("error getting the root node: err=%w", err)
	}
	index, node, _, err := n.findKey(key, true)
	if err != nil {
		return nil, fmt.Errorf("error finding the key in tree: err=%w", err)
	}
	if index == -1 {
		return nil, nil
	}
	return node.items[index], nil
}

func (c *Collection) getNodes(indexes []int) ([]*Node, error) {
	root, err := c.dal.getNode(c.root)
	if err != nil {
		return nil, fmt.Errorf("error getting the root node: err=%w", err)
	}
	nodes := []*Node{root}
	child := root
	for i := 1; i < len(indexes); i++ {
		child, err := c.dal.getNode(child.childNodes[indexes[i]])
		if err != nil {
			return nil, fmt.Errorf("error getting child nodes: err=%w", err)
		}
		nodes = append(nodes, child)
	}
	return nodes, nil
}

func (c *Collection) Put(key []byte, val []byte) error {
	item := NewItem(key, val)

	// On first insertion, the root node does not exist so it should be created
	root := new(Node)
	var err error
	if c.root == 0 {
		node := c.dal.newNode([]*Item{item}, []pagenum{})

		root, err = c.dal.writeNode(node)
		if err != nil {
			return fmt.Errorf("error writing node to disk. err=%w", err)
		}
		c.root = root.pageNum
		return nil
	} else {
		root, err = c.dal.getNode(c.root)
		if err != nil {
			return fmt.Errorf("error getting root from disk. err=%w", err)
		}
	}
	// Find the path to the node where the insertion should happen
	insertIdx, nodeToInsertIn, ancestorIdxs, err := root.findKey(item.key, false)
	if err != nil {
		return fmt.Errorf("error finding key=%s. err=%w", item.key, err)
	}
	if nodeToInsertIn.items != nil &&
		insertIdx < len(nodeToInsertIn.items) && bytes.Compare(nodeToInsertIn.items[insertIdx].key, key) == 0 {
		// If key already exists
		nodeToInsertIn.items[insertIdx] = item
	} else {
		// Add item to the leaf node
		nodeToInsertIn.addItem(item, insertIdx)
	}
	_, err = c.dal.writeNode(nodeToInsertIn)
	if err != nil {
		return fmt.Errorf("error writing node to disk. err=%w", err)
	}
	ancestors, err := c.getNodes(ancestorIdxs)
	if err != nil {
		return fmt.Errorf("error fetching ancestors. err=%w", err)
	}
	// Rebalance the nodes all the way up. Start from one node before the last and
	// go all the way up. Exclude root
	for i := len(ancestors) - 2; i >= 0; i-- {
		pnode, cnode := ancestors[i], ancestors[i+1]
		nodeIndex := ancestorIdxs[i+1]

		if cnode.isOverPopulated() {
			pnode.split(cnode, nodeIndex)
		}
	}
	rootNode := ancestors[0]

	if rootNode.isOverPopulated() {
		newRoot := c.dal.newNode([]*Item{}, []pagenum{rootNode.pageNum})
		newRoot.split(rootNode, 0)

		// commit newly created node
		newRoot, err = c.dal.writeNode(newRoot)
		if err != nil {
			return fmt.Errorf("error writing new root node to disk: err=%w", err)
		}
		c.root = newRoot.pageNum
	}
	return nil
}
