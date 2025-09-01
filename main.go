package main

func main() {
	dal, _ := NewDataAccessLayer("db.db")

	pg := dal.AllocateEmptyPage()
	pg.Num = dal.Freelist.GetNextPage()
	copy(pg.Data, "data")

	// commit the data
	_ = dal.WritePage(pg)
	_, _ = dal.WriteFreeList()

	// close the db
	_ = dal.Close()

	// freelist state should be saved, so we expect it to write
	// to page 3 instead of overwriting page 2
	dal, _ = NewDataAccessLayer("db.db")
	pg = dal.AllocateEmptyPage()
	pg.Num = dal.Freelist.GetNextPage()
	copy(pg.Data, "data3")
	_ = dal.WritePage(pg)

	// create the page and free it so the released pages will be
	// updated
	pageNum := dal.Freelist.GetNextPage()
	dal.Freelist.ReleasePage(pageNum)

	// commit above change
	_, _ = dal.WriteFreeList()
}
