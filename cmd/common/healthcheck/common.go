package healthcheck

const HealthCheckPort = "8080"
const HealthCheckEndpoint = "/health-check"
const HealthCheckStatusCode = 200

var HealthCheckResponse = map[string]string{"Status": "Ok"}
