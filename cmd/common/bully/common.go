package bully

const BullyPort = "10001"
const LeaderEndpoint = "leader"
const ElectionEndpoint = "election"

type LeaderResponse struct {
	Leader 		string
}

func leaderMessage(instance string) LeaderResponse {
	return LeaderResponse{Leader: instance}
}