FROM golang:1.26-alpine AS builder
WORKDIR /build
COPY src/psearch/serving/ ./
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /psearch-server ./cmd/server

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /psearch-server /app/psearch-server
ENV PORT=8080
ENV ENVIRONMENT=production
ENV PROJECT_ID=psearch-project
ENV REGION=us-central1
ENV SPANNER_INSTANCE_ID=psearch-instance
ENV SPANNER_DATABASE_ID=psearch-db
EXPOSE 8080
CMD ["/app/psearch-server"]
