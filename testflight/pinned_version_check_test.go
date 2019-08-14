package testflight_test

import (
	uuid "github.com/nu7hatch/gouuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("A resource pinned with a version during initial set of the pipeline", func() {
	Context("when a resource is pinned in the pipeline config before the check is run", func() {
		BeforeEach(func() {
			hash, err := uuid.NewV4()
			Expect(err).ToNot(HaveOccurred())

			setAndUnpausePipeline(
				"fixtures/pinned-resource-simple-trigger.yml",
				"-v", "hash="+hash.String(),
				"-y", `pinned_resource_version={"version":"v1"}`,
				"-v", "version_config=nil",
			)
		})

		It("should be able to check the resource", func() {
			check := fly("check-resource", "-r", inPipeline("some-resource"), "-w")
			Expect(check).To(gbytes.Say("some-resource.*succeeded"))

			watch := fly("trigger-job", "-j", inPipeline("some-passing-job"), "-w")
			Expect(watch).To(gbytes.Say("v1"))
		})
	})
})
