package main

import (
	"fmt"
	"strings"
	"time"
)

const (
// TimestampFormat is JDBC compliant timestamp format
// TimestampFormat = "2006-01-02 15:04:05.999999999"
// DateFormat      = "2006-01-02"
)

func parseCEDate(value string, location *time.Location) (time.Time, error) {
	// const layout = "_2 Jan 2006"
	const layout = "2006-01-02 15:04:05.999999999"
	date, err := time.ParseInLocation(layout, value, location)
	if err == nil {
		return date, err
	}
	perr, ok := err.(*time.ParseError)
	if !ok {
		return time.Time{}, err
	}
	if perr.LayoutElem != "2006" {
		return time.Time{}, err
	}
	if !strings.HasPrefix(perr.ValueElem, "-") {
		return time.Time{}, err
	}
	value = strings.Replace(value, perr.ValueElem, perr.ValueElem[1:], 1)
	date, derr := time.ParseInLocation(layout, value, location)
	if derr != nil {
		return time.Time{}, err
	}
	return date.AddDate(-2*date.Year(), 0, 0), derr
}

func main() {
	loc, err := time.LoadLocation("Canada/Pacific")
	if err != nil {
		return
	}
	// fmt.Println(parseCEDate("3 Mar 1500"))
	// fmt.Println(parseCEDate("3 Mar -1500"))
	fmt.Println(parseCEDate("1500-03-28 00:00:00", loc))
	fmt.Println(parseCEDate("-1500-03-28 00:00:00", loc))

	loc, err = time.LoadLocation("Canada/Mountain")
	if err != nil {
		return
	}
	// fmt.Println(parseCEDate("3 Mar 1500"))
	// fmt.Println(parseCEDate("3 Mar -1500"))
	fmt.Println(parseCEDate("1500-03-28 00:00:00", loc))
	fmt.Println(parseCEDate("-1500-03-28 00:00:00", loc))

	zt, _ := parseCEDate("0000-02-22 00:00:00", loc)
	fmt.Println(zt)

	zt2 := zt.AddDate(0, 0, -53)
	x := fmt.Sprint(zt2)
	fmt.Println(x)

	fmt.Println(parseCEDate("-0001-12-31 00:00:00", loc))
}
