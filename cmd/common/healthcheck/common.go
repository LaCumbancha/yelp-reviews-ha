package healthcheck

const HealthCheckPort = "10000"
const HealthCheckEndpoint = "health-check"
const HealthCheckStatusCode = 200

var HealthCheckResponse = map[string]string{"Status": "Ok"}
