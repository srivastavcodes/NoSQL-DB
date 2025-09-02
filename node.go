package main

import (
	"bytes"
	"encoding/binary"
)

type Item struct {
	key []byte
	val []byte
}

type Node struct {
	DALayer    *DALayer
	pageNum    pagenum
	items      []*Item
	childNodes []pagenum
}

func NewEmptyNode() *Node { return &Node{} }

func NewItem(key []byte, val []byte) *Item {
	return &Item{key: key, val: val}
}

func (n *Node) isLeaf() bool { return len(n.childNodes) == 0 }

func (n *Node) writeNode(node *Node) *Node {
	node, _ = n.DALayer.writeNode(node)
	return node
}

func (n *Node) writeNodes(nodes ...*Node) {
	for _, node := range nodes {
		n.writeNode(node)
	}
}

func (n *Node) getNode(pgnum pagenum) (*Node, error) {
	return n.DALayer.getNode(pgnum)
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

// findKey searches for a key inside the tree. Once the key is found,
// the parent node and the correct index are returned so the key
// itself can be assessed in the following way parent[index].
// If the key isn't found, a falsey answer is found.
func (n *Node) findKey(key []byte) (int, *Node, error) {
	index, node, err := findKeyHelper(n, key)
	if err != nil {
		return -1, nil, err
	}
	return index, node, nil
}

func findKeyHelper(node *Node, key []byte) (int, *Node, error) {
	// search for the key inside the node
	wasFound, index := node.findKeyInNode(key)
	if wasFound {
		return index, node, nil
	}
	// if we reached a leaf node and the key wasn't found, it
	// means the key doesn't exist
	if node.isLeaf() {
		return -1, nil, nil
	}
	// else keep searching the tree recursively
	nextChild, err := node.getNode(node.childNodes[index])
	if err != nil {
		return -1, nil, err
	}
	return findKeyHelper(nextChild, key)
}

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
