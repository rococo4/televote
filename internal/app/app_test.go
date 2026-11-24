package app

import (
	"strings"
	"testing"
	"time"
)

func TestConfiguredCloseGrace(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  time.Duration
		valid bool
	}{
		{"", 5 * time.Second, true},
		{"1500ms", 1500 * time.Millisecond, true},
		{"0s", 0, true},
		{"-1s", 0, false},
		{"500ns", 0, false},
		{"10", 0, false},
	} {
		t.Run("value="+tc.value, func(t *testing.T) {
			t.Setenv("POLL_CLOSE_GRACE_PERIOD", tc.value)
			grace, err := configuredCloseGrace()
			if grace != tc.want || (err == nil) != tc.valid {
				t.Fatalf("got (%s, %v), want %s, valid=%t", grace, err, tc.want, tc.valid)
			}
			if err != nil && !strings.Contains(err.Error(), "POLL_CLOSE_GRACE_PERIOD") {
				t.Fatalf("startup error omits setting name: %v", err)
			}
		})
	}
}
