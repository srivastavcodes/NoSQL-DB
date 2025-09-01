package main

import "encoding/binary"

const initialPage = 0

type Freelist struct {
	// holds the maximum pages allocated. maxPage*pageSize=fileSize
	maxPage PageNum
	// releasedPages are pages that were previously allocated but are now free
	releasedPages []PageNum
}

func newFreeList() *Freelist {
	return &Freelist{maxPage: initialPage,
		releasedPages: make([]PageNum, 0),
	}
}

func (fl *Freelist) GetNextPage() PageNum {
	if len(fl.releasedPages) != 0 {
		pageId := fl.releasedPages[len(fl.releasedPages)-1]
		fl.releasedPages = fl.releasedPages[:len(fl.releasedPages)-1]
		return pageId
	}
	fl.maxPage += 1
	return fl.maxPage
}

func (fl *Freelist) ReleasePage(page PageNum) {
	fl.releasedPages = append(fl.releasedPages, page)
}

func (fl *Freelist) serialize(buf []byte) []byte {
	pos := 0

	binary.LittleEndian.PutUint16(buf[pos:],
		uint16(fl.maxPage),
	)
	pos += 2
	binary.LittleEndian.PutUint16(buf[pos:],
		uint16(len(fl.releasedPages)),
	)
	pos += 2
	for _, page := range fl.releasedPages {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(page))
		pos += pageNumSize
	}
	return buf
}

func (fl *Freelist) deserialize(buf []byte) {
	pos := 0

	fl.maxPage = PageNum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	releasedPageCount := int(PageNum(binary.LittleEndian.Uint16(buf[pos:])))
	pos += 2
	for i := 0; i < releasedPageCount; i++ {
		fl.releasedPages = append(
			fl.releasedPages,
			PageNum(binary.LittleEndian.Uint64(buf[pos:])),
		)
		pos += pageNumSize
	}
}
