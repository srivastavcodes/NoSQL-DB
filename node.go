package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type Item struct {
	key []byte
	val []byte
}

type Node struct {
	pageNum    pagenum
	items      []*Item
	childNodes []pagenum
	dal        *DALayer
}

func NewEmptyNode() *Node { return &Node{} }

func NewItem(key []byte, val []byte) *Item {
	return &Item{key: key, val: val}
}

func (n *Node) isLeaf() bool { return len(n.childNodes) == 0 }

func (n *Node) writeNode(node *Node) *Node {
	node, _ = n.dal.writeNode(node)
	return node
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pgnum pagenum) (*Node, error) {
	return n.dal.getNode(pgnum)
}

func (n *Node) isOverPopulated() bool {
	return n.dal.isOverPopulated(n)
}

func (n *Node) canSpareAnElement() bool {
	splitIndex := n.dal.getSplitIndex(n)
	return splitIndex != -1
}

func (n *Node) isUnderPopulated() bool {
	return n.dal.isUnderPopulated(n)
}

func (n *Node) serialize(buf []byte) []byte {
	leftpos, rightpos := 0, len(buf)-1

	// add page header: is leaf, key-val pair count, node num
	// -> isLeaf?
	isLeaf := n.isLeaf()

	var bitSetVar uint64
	if isLeaf {
		bitSetVar = 1
	}
	buf[leftpos] = byte(bitSetVar)
	leftpos += 1

	// -> key-val pair count
	binary.LittleEndian.PutUint16(buf[leftpos:], uint16(len(n.items)))
	leftpos += 2

	for i := 0; i < len(n.items); i++ {
		item := n.items[i]

		if !isLeaf {
			childNode := n.childNodes[i]
			// write the child page as a fixed size of 8 bytes
			binary.LittleEndian.PutUint64(buf[leftpos:], uint64(childNode))
			leftpos += pageNumSize
		}
		klen := len(item.key)
		vlen := len(item.val)

		// -> write offset
		offset := rightpos - klen - vlen - 2
		binary.LittleEndian.PutUint16(buf[leftpos:], uint16(offset))
		leftpos += 2

		rightpos -= vlen
		copy(buf[rightpos:], item.val)

		rightpos -= 1
		buf[rightpos] = byte(vlen)

		rightpos -= klen
		copy(buf[rightpos:], item.key)

		rightpos -= 1
		buf[rightpos] = byte(klen)
	}
	if !isLeaf {
		// write the last child node
		lastChildNode := n.childNodes[len(n.childNodes)-1]
		// write the child page as a fixed size of 8 bytes
		binary.LittleEndian.PutUint64(buf[leftpos:], uint64(lastChildNode))
	}
	return buf
}

func (n *Node) deserialize(buf []byte) {
	leftpos := 0

	// Read header: isLeaf?
	isLeaf := uint16(buf[leftpos])
	leftpos += 1

	itemsCount := int(binary.LittleEndian.Uint16(buf[leftpos:]))
	leftpos += 2

	// Read body
	for i := 0; i < itemsCount; i++ {
		if isLeaf == 0 { // isFalse
			pageNum := binary.LittleEndian.Uint64(buf[leftpos:])
			leftpos += pageNumSize
			n.childNodes = append(n.childNodes, pagenum(pageNum))
		}
		// Read offset
		offset := binary.LittleEndian.Uint16(buf[leftpos:])
		leftpos += 2

		klen := uint16(buf[int(offset)])
		offset += 1

		key := buf[offset : offset+klen]
		offset += klen

		vlen := uint16(buf[int(offset)])
		offset += 1

		val := buf[offset : offset+vlen]
		offset += vlen

		n.items = append(n.items, NewItem(key, val))
	}
	if isLeaf == 0 { // isFalse
		pageNum := pagenum(binary.LittleEndian.Uint64(buf[leftpos:]))
		n.childNodes = append(n.childNodes, pageNum)
	}
}

// elementSize returns the size of a key-value-child triplet at a given
// index. If the node is a leaf, then the size of key-value pair is
// returned. It's assumed idx <= len(n.items)
func (n *Node) elementSize(idx int) int {
	size := 0
	size += len(n.items[idx].key)
	size += len(n.items[idx].val)
	size += pageNumSize
	return size
}

func (n *Node) nodeSize() int {
	size := 0
	size += nodeHeaderSize
	for idx := range n.items {
		size += n.elementSize(idx)
	}
	size += pageNumSize // add last page
	return size
}

// addItem inserts the item at the (index) by shifting the elements
// left and right accordingly
func (n *Node) addItem(item *Item, index int) int {
	if len(n.items) == index { // items is empty, or at last index
		n.items = append(n.items, item)
		return index
	}
	n.items = append(n.items[:index+1], n.items[index:]...)
	n.items[index] = item
	return index
}

// findKey searches for a key inside the tree. Once the key is found, the parent node
// and the correct index are returned so the key itself can be assessed in the
// following way parent[index]. A list of node ancestors (not including the curr node
// itself) is also returned.
//
// If the key isn't present, we can return from 2 options. If exact is true, it means
// we expect findKey to find a key, so a falsy answer is returned; if exact is false
// then it locates where a key should be inserted so the position is returned.
func (n *Node) findKey(key []byte, exact bool) (int, *Node, []int, error) {
	ancestorIdxs := []int{0} // index of root

	index, node, err := findKeyHelper(n, key, exact, &ancestorIdxs)
	if err != nil {
		return -1, nil, nil, fmt.Errorf("error in findKeyHelper. error=%w", err)
	}
	return index, node, ancestorIdxs, nil
}

func findKeyHelper(node *Node, key []byte, exact bool, ancestorIdxs *[]int) (int, *Node, error) {
	// search for the key inside the node
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}
	// if we reached a leaf node and the key wasn't found, it
	// means the key doesn't exist
	if node.isLeaf() {
		if exact {
			return -1, nil, nil
		}
		return index, node, nil
	}
	*ancestorIdxs = append(*ancestorIdxs, index)

	// else keep searching the tree recursively
	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key, exact, ancestorIdxs)
}

// findKeyInNode iterates all the items and finds the key. If the key is
// found, then return the index where it should've been (the first idx
// where the key is greater than its previous)
func (n *Node) findKeyInNode(key []byte) (bool, int) {
	for i, item := range n.items {
		res := bytes.Compare(item.key, key)

		if res == 0 { // keys match
			return true, i
		}
		// The key is bigger than the previous item, meaning
		// it doesn't exist in this node, but may exist in
		// child nodes
		if res == 1 {
			return false, i
		}
	}
	// The key isn't bigger than any items which means we are at
	// the last index
	return false, len(n.items)
}

func (n *Node) split(node *Node, index int) {
	splitIndex := node.dal.getSplitIndex(node)

	middleItem := node.items[splitIndex]
	newNode := new(Node)

	// TODO -> draw this shit down on paper
	if node.isLeaf() {
		newNode = n.writeNode(n.dal.newNode(node.items[splitIndex+1:], []pagenum{}))
		node.items = node.items[:splitIndex]
	} else {
		newNode = n.writeNode(n.dal.newNode(node.items[splitIndex+1:], node.childNodes[splitIndex+1:]))
		node.items = node.items[:splitIndex]
		node.childNodes = node.childNodes[:splitIndex+1]
	}
	n.addItem(middleItem, index)

	if len(n.childNodes) == index+1 {
		n.childNodes = append(n.childNodes, newNode.pageNum)
	} else {
		n.childNodes = append(n.childNodes[:index+1], n.childNodes[index:]...)
		n.childNodes[index+1] = newNode.pageNum
	}
	n.writeNodes(n, node)
}
