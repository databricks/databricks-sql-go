package u2m

import (
	"github.com/lestrrat-go/jwx/jwt"
)

func CustomFieldString(token jwt.Token, key string) string {
	if value, ok := token.Get(key); ok {
		if str, ok := value.(string); ok {
			return str
		}
	}
	return ""
}

func CustomFieldStrings(token jwt.Token, key string) []string {
	if value, ok := token.Get(key); ok {
		if str, ok := value.([]string); ok {
			return str
		}
	}
	return []string{}
}

func CustomFieldBool(token jwt.Token, key string) bool {
	if value, ok := token.Get(key); ok {
		if str, ok := value.(bool); ok {
			return str
		}
	}
	return false
}
