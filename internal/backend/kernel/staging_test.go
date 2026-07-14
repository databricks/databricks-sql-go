package kernel

import "testing"

func TestIsStagingStatement(t *testing.T) {
	staging := []string{
		"PUT '/tmp/f.csv' INTO '/Volumes/main/s/e/f.csv' OVERWRITE",
		"GET '/Volumes/main/s/e/f.csv' TO 'local.csv'",
		"REMOVE '/Volumes/main/s/e/f.csv'",
		"  put '/tmp/f' into '/Volumes/x'",                  // leading space + lowercase
		"Get\t'/Volumes/x' to 'y'",                          // tab after keyword, mixed case
		"/* audit */ PUT '/local' INTO '/Volumes/main/s/v'", // leading block comment
		"/* a */ /* b */ PUT '/l' INTO '/Volumes/x'",        // multiple block comments
		"-- upload the file\nPUT '/l' INTO '/Volumes/x'",    // leading line comment, then staging
		"  /* c */\n  -- d\n  REMOVE '/Volumes/x'",          // mixed comments + whitespace
	}
	for _, sql := range staging {
		if !isStagingStatement(sql) {
			t.Errorf("expected staging: %q", sql)
		}
	}

	notStaging := []string{
		"SELECT 1",
		"PUTS ON A SHOW",           // PUT is a prefix of an identifier, not a token
		"GETTER()",                 // likewise for GET
		"REMOVED_AT FROM t",        // likewise for REMOVE
		"INSERT INTO t VALUES (1)", // contains INTO but is not staging
		"",                         // empty
		"-- PUT '/x' INTO '/y'",    // whole line commented out; nothing after
		"/* PUT '/x' INTO '/y' */", // whole statement inside a block comment
		"SELECT 'PUT ' AS col",     // keyword only inside a literal
		"/* unterminated PUT '/x'", // unterminated block comment consumes the rest
		"/* c */ SELECT 1",         // real leading comment, but not a staging command
	}
	for _, sql := range notStaging {
		if isStagingStatement(sql) {
			t.Errorf("did not expect staging: %q", sql)
		}
	}
}
