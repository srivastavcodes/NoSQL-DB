package main

import (
	"errors"
	"fmt"
	"os"
)

type pagenum int64

type Page struct {
	Num  pagenum
	Data []byte
}

type DALayer struct {
	Freelist *Freelist
	Meta     *Metadata

	file     *os.File
	pageSize int
}

func NewDataAccessLayer(path string) (*DALayer, error) {
	dal := &DALayer{
		Meta:     NewEmptyMeta(),
		pageSize: 4096,
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
		dal.Meta = meta

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
		dal.Meta.FreeListPage = dal.Freelist.getNextPage()

		_, err = dal.WriteFreeList()
		if err != nil {
			return nil, err
		}
		_, err = dal.writeMetadata(dal.Meta)
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
	pg, err := dal.ReadPage(dal.Meta.FreeListPage)
	if err != nil {
		return nil, err
	}
	freelist := newFreeList()

	freelist.deserialize(pg.Data)
	return freelist, nil
}

func (dal *DALayer) WriteFreeList() (*Page, error) {
	pg := dal.AllocateEmptyPage()

	pg.Num = dal.Meta.FreeListPage
	dal.Freelist.serialize(pg.Data)

	err := dal.WritePage(pg)
	if err != nil {
		return nil, err
	}
	dal.Meta.FreeListPage = pg.Num
	return pg, nil
}

func (dal *DALayer) getNode(pgnum pagenum) (*Node, error) {
	pg, err := dal.ReadPage(pgnum)
	if err != nil {
		return nil, err
	}
	node := NewEmptyNode()

	node.deserialize(pg.Data)
	node.pageNum = pgnum
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

func (dal *DALayer) deleteNode(pgnum pagenum) {
	dal.Freelist.ReleasePage(pgnum)
}
