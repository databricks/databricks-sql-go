package utils

import "fmt"

type Guid []byte

func (b Guid) String() string {
	bts := []byte(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", bts[0:4], bts[4:6], bts[6:8], bts[8:10], bts[10:16])
}
