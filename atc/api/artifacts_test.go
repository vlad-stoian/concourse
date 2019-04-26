package api_test

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/concourse/concourse/atc"
	"github.com/concourse/concourse/atc/api/accessor/accessorfakes"
	"github.com/concourse/concourse/atc/db/dbfakes"
	"github.com/concourse/concourse/atc/worker/workerfakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Artifacts API", func() {
	var fakeaccess *accessorfakes.FakeAccess

	BeforeEach(func() {
		fakeaccess = new(accessorfakes.FakeAccess)
	})

	JustBeforeEach(func() {
		fakeAccessor.CreateReturns(fakeaccess)
	})

	Describe("POST /api/v1/teams/:team_name/artifacts", func() {
		var request *http.Request
		var response *http.Response

		BeforeEach(func() {
			fakeaccess = new(accessorfakes.FakeAccess)
			fakeaccess.IsAuthenticatedReturns(true)

			fakeAccessor.CreateReturns(fakeaccess)
		})

		JustBeforeEach(func() {
			var err error
			request, err = http.NewRequest("POST", server.URL+"/api/v1/teams/some-team/artifacts", bytes.NewBuffer([]byte("some-data")))
			Expect(err).NotTo(HaveOccurred())

			request.Header.Set("Content-Type", "application/json")

			response, err = client.Do(request)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when not authenticated", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthenticatedReturns(false)
			})

			It("returns 401 Unauthorized", func() {
				Expect(response.StatusCode).To(Equal(http.StatusUnauthorized))
			})
		})

		Context("when not authorized", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthorizedReturns(false)
			})

			It("returns 403 Forbidden", func() {
				Expect(response.StatusCode).To(Equal(http.StatusForbidden))
			})
		})

		Context("when authorized", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthorizedReturns(true)
			})

			Context("when creating an artifact fails", func() {
				BeforeEach(func() {
					fakeWorkerClient.CreateArtifactReturns(atc.WorkerArtifact{}, errors.New("nope"))
				})

				It("returns 500 InternalServerError", func() {
					Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
				})
			})

			Context("when creating an artifact succeeds", func() {
				BeforeEach(func() {
					fakeWorkerClient.CreateArtifactReturns(atc.WorkerArtifact{
						ID:        0,
						Name:      "",
						BuildID:   0,
						CreatedAt: 42,
					}, nil)
				})

				Context("when storing data to the artifact fails", func() {
					BeforeEach(func() {
						fakeWorkerClient.StoreReturns(errors.New("nope"))
					})

					It("returns 500 InternalServerError", func() {
						Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
					})
				})

				Context("when storing data to the artifact succeeds", func() {
					It("streams in the user contents to the artifact", func() {
						Expect(fakeWorkerClient.StoreCallCount()).To(Equal(1))
					})

					Context("when the request succeeds", func() {
						BeforeEach(func() {
							fakeWorkerClient.StoreReturns(nil)
						})

						It("returns 201 Created", func() {
							Expect(response.StatusCode).To(Equal(http.StatusCreated))
						})

						It("returns Content-Type 'application/json'", func() {
							Expect(response.Header.Get("Content-Type")).To(Equal("application/json"))
						})

						It("returns the artifact record", func() {
							body, err := ioutil.ReadAll(response.Body)
							Expect(err).NotTo(HaveOccurred())

							Expect(body).To(MatchJSON(`{
									"id": 0,
									"name": "",
									"build_id": 0,
									"created_at": 42
								}`))
						})
					})
				})
			})
		})
	})

	Describe("GET /api/v1/teams/:team_name/artifacts/:artifact_id", func() {
		var response *http.Response

		BeforeEach(func() {
			fakeaccess = new(accessorfakes.FakeAccess)
			fakeaccess.IsAuthenticatedReturns(true)

			fakeAccessor.CreateReturns(fakeaccess)
		})

		JustBeforeEach(func() {
			var err error
			response, err = http.Get(server.URL + "/api/v1/teams/some-team/artifacts/18")
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when not authenticated", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthenticatedReturns(false)
			})

			It("returns 401 Unauthorized", func() {
				Expect(response.StatusCode).To(Equal(http.StatusUnauthorized))
			})
		})

		Context("when not authorized", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthorizedReturns(false)
			})

			It("returns 403 Forbidden", func() {
				Expect(response.StatusCode).To(Equal(http.StatusForbidden))
			})
		})

		Context("when authorized", func() {
			BeforeEach(func() {
				fakeaccess.IsAuthorizedReturns(true)
			})

			It("uses the artifactID to fetch the db volume record", func() {
				Expect(dbTeam.FindVolumeForWorkerArtifactCallCount()).To(Equal(1))

				artifactID := dbTeam.FindVolumeForWorkerArtifactArgsForCall(0)
				Expect(artifactID).To(Equal(18))
			})

			Context("when retrieving db artifact volume fails", func() {
				BeforeEach(func() {
					dbTeam.FindVolumeForWorkerArtifactReturns(nil, false, errors.New("nope"))
				})

				It("errors", func() {
					Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
				})
			})

			Context("when the db artifact volume is not found", func() {
				BeforeEach(func() {
					dbTeam.FindVolumeForWorkerArtifactReturns(nil, false, nil)
				})

				It("returns 404", func() {
					Expect(response.StatusCode).To(Equal(http.StatusNotFound))
				})
			})

			Context("when the db artifact volume is found", func() {
				var fakeVolume *dbfakes.FakeCreatedVolume

				BeforeEach(func() {
					fakeVolume = new(dbfakes.FakeCreatedVolume)
					fakeVolume.HandleReturns("some-handle")

					dbTeam.FindVolumeForWorkerArtifactReturns(fakeVolume, true, nil)
				})

				It("uses the volume handle to lookup the worker volume", func() {
					Expect(fakeWorkerClient.FindVolumeCallCount()).To(Equal(1))

					_, teamID, handle := fakeWorkerClient.FindVolumeArgsForCall(0)
					Expect(handle).To(Equal("some-handle"))
					Expect(teamID).To(Equal(734))
				})

				Context("when the worker client errors", func() {
					BeforeEach(func() {
						fakeWorkerClient.FindVolumeReturns(nil, false, errors.New("nope"))
					})

					It("returns 500", func() {
						Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
					})
				})

				Context("when the worker client can't find the volume", func() {
					BeforeEach(func() {
						fakeWorkerClient.FindVolumeReturns(nil, false, nil)
					})

					It("returns 404", func() {
						Expect(response.StatusCode).To(Equal(http.StatusNotFound))
					})
				})

				Context("when the worker client finds the volume", func() {
					var fakeWorkerVolume *workerfakes.FakeVolume

					BeforeEach(func() {
						reader := ioutil.NopCloser(bytes.NewReader([]byte("")))

						fakeWorkerVolume = new(workerfakes.FakeVolume)
						fakeWorkerVolume.StreamOutReturns(reader, nil)

						fakeWorkerClient.FindVolumeReturns(fakeWorkerVolume, true, nil)
					})

					It("streams out the contents of the volume from the root path", func() {
						Expect(fakeWorkerVolume.StreamOutCallCount()).To(Equal(1))

						path := fakeWorkerVolume.StreamOutArgsForCall(0)
						Expect(path).To(Equal("/"))
					})

					Context("when streaming volume contents fails", func() {
						BeforeEach(func() {
							fakeWorkerVolume.StreamOutReturns(nil, errors.New("nope"))
						})

						It("returns 500", func() {
							Expect(response.StatusCode).To(Equal(http.StatusInternalServerError))
						})
					})

					Context("when streaming volume contents succeeds", func() {
						BeforeEach(func() {
							reader := ioutil.NopCloser(bytes.NewReader([]byte("some-content")))
							fakeWorkerVolume.StreamOutReturns(reader, nil)
						})

						It("returns 200", func() {
							Expect(response.StatusCode).To(Equal(http.StatusOK))
						})

						It("returns Content-Type 'application/octet-stream'", func() {
							Expect(response.Header.Get("Content-Type")).To(Equal("application/octet-stream"))
						})

						It("returns the contents of the volume", func() {
							Expect(ioutil.ReadAll(response.Body)).To(Equal([]byte("some-content")))
						})
					})
				})
			})
		})
	})
})
