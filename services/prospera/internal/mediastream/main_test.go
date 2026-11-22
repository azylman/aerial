package mediastream

import (
	"os"
	"testing"

	"github.com/azylman/aerial/pkg/pgtest"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}
