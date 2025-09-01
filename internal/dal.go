package internal

import (
	"fmt"
	"os"
)

type PageNum int64

type Page struct {
	Num  PageNum
	Data []byte
}

type Dal struct {
	*Freelist

	file     *os.File
	pageSize int
}

func NewDal(path string, pageSize int) (*Dal, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}
	dal := &Dal{
		Freelist: newFreeList(),
		file:     file,
		pageSize: pageSize,
	}
	return dal, nil
}

func (d *Dal) Close() error {
	if d.file != nil {
		err := d.file.Close()
		if err != nil {
			return fmt.Errorf("could not close file: %w", err)
		}
		d.file = nil
	}
	return nil
}

func (d *Dal) AllocateEmptyPage() *Page {
	return &Page{
		Data: make([]byte, d.pageSize),
	}
}

func (d *Dal) ReadPage(pageNum PageNum) (*Page, error) {
	pg := d.AllocateEmptyPage()

	offset := int(pageNum) * d.pageSize

	_, err := d.file.ReadAt(pg.Data, int64(offset))
	if err != nil {
		return nil, err
	}
	return pg, nil
}

func (d *Dal) WritePage(pg *Page) error {
	offset := int64(pg.Num) * int64(d.pageSize)
	_, err := d.file.WriteAt(pg.Data, offset)
	return err
}
