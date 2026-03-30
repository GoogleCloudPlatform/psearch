# 
# Copyright 2025 Google LLC
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     https://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# ---- Build Stage ----
# Use the Go version confirmed to work with dependencies
FROM golang:1.26-alpine AS builder

# Install build dependencies (git for private modules if needed, ca-certs for https downloads)
RUN apk add --no-cache git ca-certificates

# Set the working directory inside the container
WORKDIR /app

# Copy the entire application source code first
COPY . .

# Initialize the module (if go.mod doesn't exist - safe to run even if it does)
# Ensures build works even if go.mod is not present in the source context
RUN go mod init psearch/serving-go || true

# Tidy dependencies - this will generate go.mod/go.sum based on source code imports
# and download necessary modules within the build container
RUN go mod tidy

# Build the application as a static binary
# - CGO_ENABLED=0 disables Cgo for static linking
# - GOOS=linux GOARCH=amd64 targets the Cloud Run environment
# - ldflags "-w -s" removes debug information and symbol table for smaller binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /app/server ./cmd/server

# ---- Final Stage ----
# Use a minimal Alpine image for the final container
FROM alpine:latest

# Import CA certificates and timezone data
RUN apk add --no-cache ca-certificates tzdata

# Create a non-root user and group for security
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Set the working directory
WORKDIR /app

# Copy the built binary from the builder stage
# Ensure correct ownership for the non-root user
COPY --from=builder --chown=appuser:appgroup /app/server /app/server

# Switch to the non-root user
USER appuser

# Expose the port the application listens on (matches config/main.go)
EXPOSE 8080

# Set the entrypoint command to run the server
ENTRYPOINT ["/app/server"]
