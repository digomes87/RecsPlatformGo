// Command minttoken issues a development JWT for smoke tests and manual
// exploration of the local stack.
package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	secret := flag.String("secret", "dev-secret-change-me", "signing secret")
	sub := flag.String("sub", "dev-user", "token subject (user id)")
	ttl := flag.Duration("ttl", time.Hour, "token lifetime")
	flag.Parse()

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":     *sub,
		"scopes":  []string{"read", "write"},
		"exp":     time.Now().Add(*ttl).Unix(),
	})
	signed, err := tok.SignedString([]byte(*secret))
	if err != nil {
		panic(err)
	}
	fmt.Println(signed)
}
