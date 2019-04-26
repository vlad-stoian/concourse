package artifactserver

import (
	"encoding/json"
	"net/http"

	"github.com/concourse/concourse/atc/db"
)

func (s *Server) CreateArtifact(team db.Team) http.Handler {
	hLog := s.logger.Session("create-artifact")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// TODO: can probably check if fly sent us an etag header
		// which we can lookup in the checksum field
		// that way we don't have to create another volume.
		artifact, err := s.workerClient.CreateArtifact(hLog, "")
		if err != nil {
			hLog.Error("failed-to-create-artifact", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		err = s.workerClient.Store(hLog, team.ID(), artifact, "/", r.Body)
		if err != nil {
			hLog.Error("failed-to-stream-artifact-contents", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)

		json.NewEncoder(w).Encode(artifact)
	})
}
