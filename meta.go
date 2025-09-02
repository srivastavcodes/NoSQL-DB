package main

import "encoding/binary"

const metaPageNum = 0

type Metadata struct {
	Root         pagenum
	FreeListPage pagenum
}

func NewEmptyMeta() *Metadata { return &Metadata{} }

func (m *Metadata) serialize(buf []byte) {
	pos := 0

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.Root))
	pos += pageNumSize

	binary.LittleEndian.PutUint64(buf[pos:], uint64(m.FreeListPage))
	pos += pageNumSize
}

func (m *Metadata) deserialize(buf []byte) {
	pos := 0

	m.Root = pagenum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize

	m.FreeListPage = pagenum(binary.LittleEndian.Uint64(buf[pos:]))
	pos += pageNumSize
}
