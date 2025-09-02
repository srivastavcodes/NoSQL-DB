package main

import "encoding/binary"

const initialPage = 0

type Freelist struct {
	// holds the maximum pages allocated. maxPage*pageSize=fileSize
	maxPage pagenum
	// releasedPages are pages that were previously allocated but are now free
	releasedPages []pagenum
}

func newFreeList() *Freelist {
	return &Freelist{maxPage: initialPage,
		releasedPages: make([]pagenum, 0),
	}
}

func (fl *Freelist) getNextPage() pagenum {
	if len(fl.releasedPages) != 0 {
		pageId := fl.releasedPages[len(fl.releasedPages)-1]
		fl.releasedPages = fl.releasedPages[:len(fl.releasedPages)-1]
		return pageId
	}
	fl.maxPage += 1
	return fl.maxPage
}

func (fl *Freelist) ReleasePage(page pagenum) {
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

	fl.maxPage = pagenum(binary.LittleEndian.Uint16(buf[pos:]))
	pos += 2
	releasedPageCount := int(pagenum(binary.LittleEndian.Uint16(buf[pos:])))
	pos += 2
	for i := 0; i < releasedPageCount; i++ {
		fl.releasedPages = append(
			fl.releasedPages,
			pagenum(binary.LittleEndian.Uint64(buf[pos:])),
		)
		pos += pageNumSize
	}
}
