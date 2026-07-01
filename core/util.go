package core

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
)

var (
	byteUnits = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"}
)

func Exit(err error) {
	fmt.Fprintln(os.Stderr, "Error:", err)
	os.Exit(-1)
}

func EmptyChar() string {
	return string([]byte{0})
}

func Min(a int, b int) int {
	if a > b {
		return b
	}
	return a
}

func Max(a int, b int) int {
	if a < b {
		return b
	}
	return a
}

func ReadableSize(s int) string {
	sf := float64(s)
	i := 0
	for sf > 1024 {
		i++
		sf /= 1024
	}
	return fmt.Sprintf("%.1f%s", sf, byteUnits[i])
}

// FormatThousands renders an integer with thousands separators,
// e.g. 1936675 -> "1,936,675".
func FormatThousands(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	pre := len(s) % 3
	var b strings.Builder
	if pre > 0 {
		b.WriteString(s[:pre])
		b.WriteByte(',')
	}
	for i := pre; i < len(s); i += 3 {
		b.WriteString(s[i : i+3])
		if i+3 < len(s) {
			b.WriteByte(',')
		}
	}
	return b.String()
}

func Interrupt(f func()) {
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt)
		s := <-c
		if s == os.Interrupt {
			fmt.Println("interrupt")
			f()
			Exit(errors.New("signal: interrupt"))
		}
	}()
}
