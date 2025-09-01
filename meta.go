package main

import "encoding/binary"

const metaPageNum = 0

type Metadata struct {
	FreeListPage PageNum
}

func NewEmptyMeta() *Metadata { return &Metadata{} }

func (m *Metadata) serialize(buf []byte) {
	pos := 0
	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.FreeListPage))
	pos += pageNumSize
}

func (m *Metadata) deserialize(buf []byte) {
	pos := 0
	m.FreeListPage = PageNum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
