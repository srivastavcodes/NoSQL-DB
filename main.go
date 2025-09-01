package main

import (
	nosql "nosql/internal"
	"os"
)

func main() {
	dal, _ := nosql.NewDal("db.db", os.Getpagesize())

	pg := dal.AllocateEmptyPage()
	pg.Num = dal.GetNextPage()
	copy(pg.Data[:], "data")

	_ = dal.WritePage(pg)
}
