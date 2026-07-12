package kernel

import "testing"

func TestQuoteIdent(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"main", "`main`"},
		{"my_schema", "`my_schema`"},
		{"", "``"},
		{"has space", "`has space`"},
		// A backtick in the identifier is doubled so it can't terminate the quote.
		{"a`b", "`a``b`"},
		// One backtick → doubled to two, wrapped in two more → four total.
		{"`", "````"},
		{"weird`;DROP", "`weird``;DROP`"},
	}
	for _, c := range cases {
		if got := quoteIdent(c.in); got != c.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
