package concourse_test

import (
	"net/http"

	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/go-concourse/concourse"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/onsi/gomega/ghttp"
)

var _ = Describe("CheckResourceType", func() {
	Context("when ATC request succeeds", func() {
		var expectedCheck atc.Check

		BeforeEach(func() {
			expectedCheck = atc.Check{
				ID:         123,
				Status:     "started",
				CreateTime: 100000000000,
				StartTime:  100000000000,
				EndTime:    100000000000,
			}

			expectedURL := "/api/v1/teams/some-team/pipelines/mypipeline/resource-types/myresource/check"
			atcServer.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", expectedURL),
					ghttp.VerifyJSON(`{"from":{"ref":"fake-ref"}}`),
					ghttp.RespondWithJSONEncoded(http.StatusOK, expectedCheck),
				),
			)
		})

		It("sends check resource request to ATC", func() {
			check, found, err := team.CheckResourceType("mypipeline", "myresource", atc.Version{"ref": "fake-ref"})
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeTrue())
			Expect(check).To(Equal(expectedCheck))

			Expect(atcServer.ReceivedRequests()).To(HaveLen(1))
		})
	})

	Context("when pipeline or resource-type does not exist", func() {
		BeforeEach(func() {
			expectedURL := "/api/v1/teams/some-team/pipelines/mypipeline/resource-types/myresource/check"
			atcServer.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", expectedURL),
					ghttp.RespondWithJSONEncoded(http.StatusNotFound, ""),
				),
			)
		})

		It("returns a ResourceNotFoundError", func() {
			_, found, err := team.CheckResourceType("mypipeline", "myresource", atc.Version{"ref": "fake-ref"})
			Expect(err).NotTo(HaveOccurred())
			Expect(found).To(BeFalse())
		})
	})

	Context("when ATC responds with an internal server error", func() {
		BeforeEach(func() {
			expectedURL := "/api/v1/teams/some-team/pipelines/mypipeline/resource-types/myresource/check"

			atcServer.AppendHandlers(
				ghttp.CombineHandlers(
					ghttp.VerifyRequest("POST", expectedURL),
					ghttp.RespondWith(http.StatusInternalServerError, "generic error"),
				),
			)
		})

		It("returns an error", func() {
			_, _, err := team.CheckResourceType("mypipeline", "myresource", atc.Version{"ref": "fake-ref"})
			Expect(err).To(HaveOccurred())

			cre, ok := err.(concourse.GenericError)
			Expect(ok).To(BeTrue())
			Expect(cre.Error()).To(Equal("generic error"))
		})
	})

})
