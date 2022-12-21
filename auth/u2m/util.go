package u2m

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os/exec"
	"runtime"

	"github.com/lestrrat-go/jwx/jwt"
)

func randString(nByte int) (string, error) {
	b := make([]byte, nByte)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func openbrowser(url string) error {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	return err
}

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
