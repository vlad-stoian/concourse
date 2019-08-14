package testflight_test

import (
	uuid "github.com/nu7hatch/gouuid"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("Configuring a resource type in a pipeline config", func() {
	var hash string

	BeforeEach(func() {
		u, err := uuid.NewV4()
		Expect(err).ToNot(HaveOccurred())

		hash = u.String()
	})

	Context("with custom resource types", func() {
		BeforeEach(func() {
			setAndUnpausePipeline("fixtures/resource-types.yml", "-v", "hash="+hash)
		})

		It("can use custom resource types for 'get', 'put', and task 'image_resource's", func() {
			watch := fly("trigger-job", "-j", inPipeline("resource-getter"), "-w")
			Expect(watch).To(gbytes.Say("fetched version: " + hash))

			watch = fly("trigger-job", "-j", inPipeline("resource-putter"), "-w")
			Expect(watch).To(gbytes.Say("pushing version: some-pushed-version"))

			watch = fly("trigger-job", "-j", inPipeline("resource-image-resourcer"), "-w")
			Expect(watch).To(gbytes.Say("MIRRORED_VERSION=image-version"))
		})

		It("can check for resources using a custom type once the parent has a version", func() {
			checkResourceType := fly("check-resource-type", "-r", inPipeline("custom-resource-type"), "-w")
			Expect(checkResourceType).To(gbytes.Say("succeeded"))

			checkResource := fly("check-resource", "-r", inPipeline("my-resource"), "-w")
			Expect(checkResource).To(gbytes.Say("succeeded"))
		})
	})

	Context("with custom resource types that have params", func() {
		BeforeEach(func() {
			setAndUnpausePipeline("fixtures/resource-types-with-params.yml", "-v", "hash="+hash)
		})

		It("can use a custom resource with parameters", func() {
			watch := fly("trigger-job", "-j", inPipeline("resource-test"), "-w")
			Expect(watch).To(gbytes.Say(hash))
		})
	})

	Context("when resource type named as base resource type", func() {
		BeforeEach(func() {
			setAndUnpausePipeline("fixtures/resource-type-named-as-base-type.yml", "-v", "hash="+hash)
		})

		It("can use custom resource type named as base resource type", func() {
			watch := fly("trigger-job", "-j", inPipeline("resource-getter"), "-w")
			Expect(watch).To(gbytes.Say("mirror-" + hash))
		})
	})
})
