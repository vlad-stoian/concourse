package resourceserver_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestResourceserver(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Resourceserver Suite")
}
