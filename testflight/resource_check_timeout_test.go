package testflight_test

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
)

var _ = Describe("A resource check which times out", func() {
	var checkDelay time.Duration

	BeforeEach(func() {
		checkDelay = 0
	})

	JustBeforeEach(func() {
		setAndUnpausePipeline(
			"fixtures/resource-check-timeouts.yml",
			"-v", "check_delay="+checkDelay.String(),
		)
	})

	Context("when check script times out", func() {
		BeforeEach(func() {
			checkDelay = time.Minute
		})

		It("prints an error and cancels the check", func() {
			check := spawnFly("check-resource", "-r", inPipeline("my-resource"), "-w")
			<-check.Exited
			Expect(check).To(gexec.Exit(1))
			Expect(check.Out).To(gbytes.Say("errored"))
			Expect(check.Out).To(gbytes.Say("Timed out"))
		})
	})

	Context("when check script finishes before timeout", func() {
		BeforeEach(func() {
			checkDelay = time.Second
		})

		It("succeeds", func() {
			fly("check-resource", "-r", inPipeline("my-resource"), "-w")
		})
	})
})
