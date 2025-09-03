package main

import (
	"errors"
	"fmt"
	"os"
)

type pagenum int64

type Options struct {
	pageSize int

	MinFillPercent float32
	MaxFillPercent float32
}

var DefaultOptions = &Options{MinFillPercent: 0.5,
	MaxFillPercent: 0.95,
}

type Page struct {
	Num  pagenum
	Data []byte
}

type DALayer struct {
	Metadata *Metadata
	Freelist *Freelist

	file *os.File

	pageSize       int
	minFillPercent float32
	maxFillPercent float32
}

func NewDataAccessLayer(path string, options *Options) (*DALayer, error) {
	dal := &DALayer{
		Metadata:       NewEmptyMeta(),
		pageSize:       options.pageSize,
		minFillPercent: options.MinFillPercent,
		maxFillPercent: options.MaxFillPercent,
	}

	if _, err := os.Stat(path); err == nil {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.Close()
			return nil, err
		}
		meta, err := dal.readMetadata()
		if err != nil {
			return nil, err
		}
		dal.Metadata = meta

		freelist, err := dal.readFreeList()
		if err != nil {
			return nil, err
		}
		dal.Freelist = freelist
	} else if errors.Is(err, os.ErrNotExist) {
		dal.file, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
		if err != nil {
			_ = dal.file.Close()
			return nil, err
		}
		dal.Freelist = newFreeList()
		dal.Metadata.FreeListPage = dal.Freelist.getNextPage()

		_, err = dal.WriteFreeList()
		if err != nil {
			return nil, err
		}
		_, err = dal.writeMetadata(dal.Metadata)
	} else {
		return nil, err
	}
	return dal, nil
}

func (dal *DALayer) Close() error {
	if dal.file != nil {
		err := dal.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %w", err)
		}
		dal.file = nil
	}
	return nil
}

func (dal *DALayer) AllocateEmptyPage() *Page {
	return &Page{
		Data: make([]byte, dal.pageSize),
	}
}

func (dal *DALayer) ReadPage(pageNum pagenum) (*Page, error) {
	pg := dal.AllocateEmptyPage()

	offset := int(pageNum) * dal.pageSize

	_, err := dal.file.ReadAt(pg.Data, int64(offset))
	if err != nil {
		return nil, err
	}
	return pg, nil
}

func (dal *DALayer) WritePage(pg *Page) error {
	offset := int64(pg.Num) * int64(dal.pageSize)
	_, err := dal.file.WriteAt(pg.Data, offset)
	return err
}

func (dal *DALayer) writeMetadata(meta *Metadata) (*Page, error) {
	pg := dal.AllocateEmptyPage()

	pg.Num = metaPageNum
	meta.serialize(pg.Data)

	err := dal.WritePage(pg)
	if err != nil {
		return nil, err
	}
	return pg, nil
}

func (dal *DALayer) readMetadata() (*Metadata, error) {
	pg, err := dal.ReadPage(metaPageNum)
	if err != nil {
		return nil, err
	}
	meta := NewEmptyMeta()

	meta.deserialize(pg.Data)
	return meta, nil
}

func (dal *DALayer) readFreeList() (*Freelist, error) {
	pg, err := dal.ReadPage(dal.Metadata.FreeListPage)
	if err != nil {
		return nil, err
	}
	freelist := newFreeList()

	freelist.deserialize(pg.Data)
	return freelist, nil
}

func (dal *DALayer) WriteFreeList() (*Page, error) {
	pg := dal.AllocateEmptyPage()

	pg.Num = dal.Metadata.FreeListPage
	dal.Freelist.serialize(pg.Data)

	err := dal.WritePage(pg)
	if err != nil {
		return nil, err
	}
	dal.Metadata.FreeListPage = pg.Num
	return pg, nil
}

func (dal *DALayer) newNode(items []*Item, childNodes []pagenum) *Node {
	node := NewEmptyNode()
	node.items = items
	node.childNodes = childNodes
	node.pageNum = dal.Freelist.getNextPage()
	node.dal = dal
	return node
}

func (dal *DALayer) deleteNode(pgnum pagenum) {
	dal.Freelist.ReleasePage(pgnum)
}

func (dal *DALayer) getNode(pgnum pagenum) (*Node, error) {
	pg, err := dal.ReadPage(pgnum)
	if err != nil {
		return nil, err
	}
	node := NewEmptyNode()

	node.deserialize(pg.Data)
	node.pageNum = pgnum

	node.dal = dal
	return node, nil
}

func (dal *DALayer) writeNode(node *Node) (*Node, error) {
	pg := dal.AllocateEmptyPage()

	if node.pageNum == 0 {
		pg.Num = dal.Freelist.getNextPage()
		node.pageNum = pg.Num
	} else {
		pg.Num = node.pageNum
	}
	pg.Data = node.serialize(pg.Data)

	err := dal.WritePage(pg)
	if err != nil {
		return nil, err
	} else {
		return node, nil
	}
}

func (dal *DALayer) maxThreshold() float32 {
	return dal.maxFillPercent * float32(dal.pageSize)
}

func (dal *DALayer) isOverPopulated(node *Node) bool {
	return float32(node.nodeSize()) > dal.maxThreshold()
}

func (dal *DALayer) minThreshold() float32 {
	return dal.minFillPercent * float32(dal.pageSize)
}

func (dal *DALayer) isUnderPopulated(node *Node) bool {
	return float32(node.nodeSize()) < dal.minThreshold()
}

// getSplitIndex should be called when performing rebalance after an item is removed.
// It checks if a node can spare an element, and if it does, then it returns the
// index where the split should happen. Otherwise -1 is returned.
func (dal *DALayer) getSplitIndex(node *Node) int {
	size := 0
	size += nodeHeaderSize

	for i := range node.items {
		size += node.elementSize(i)
		// if we have big enough page size (more than min), and didn't reach
		// the last node, which means we can spare an element
		if float32(size) > dal.minThreshold() && i < len(node.items)-1 {
			return i + 1
		}
	}
	return -1
}
