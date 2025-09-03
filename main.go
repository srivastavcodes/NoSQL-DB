package main

import (
	"fmt"
)

func main() {
	options := &Options{
		pageSize:       4096,
		MinFillPercent: 0.0125,
		MaxFillPercent: 0.025,
	}
	dal, _ := NewDataAccessLayer("./mainTest", options)

	c := NewCollection([]byte("collection1"), dal.Metadata.Root)
	c.dal = dal

	_ = c.Put([]byte("Key1"), []byte("Value1"))
	_ = c.Put([]byte("Key2"), []byte("Value2"))
	_ = c.Put([]byte("Key3"), []byte("Value3"))
	_ = c.Put([]byte("Key4"), []byte("Value4"))
	_ = c.Put([]byte("Key5"), []byte("Value5"))
	_ = c.Put([]byte("Key6"), []byte("Value6"))
	item, _ := c.Find([]byte("Key1"))

	fmt.Printf("key is: %s, value is: %s\n", item.key, item.val)
	_ = dal.Close()
}
