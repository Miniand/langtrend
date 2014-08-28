package period

import (
	"reflect"
	"testing"
)

func assertEquals(t *testing.T, expected interface{}, actual interface{}) {
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("Expected:\n%#v\n\nActual:\n%#v", expected, actual)
	}
}
