package internal

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

func (fl *Freelist) releasePage(page PageNum) {
	fl.releasedPages = append(fl.releasedPages, page)
}
