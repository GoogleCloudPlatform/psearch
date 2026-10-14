/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package services

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"google.golang.org/genai"
	"psearch/serving-go/internal/config"
)

// QueryEmbedder produces query embeddings for vector retrieval.
// It is an interface so SpannerService can be unit-tested without network access.
type QueryEmbedder interface {
	EmbedQuery(ctx context.Context, text string) ([]float32, error)
}

// embedContentFunc abstracts the Gen AI SDK call (genai.Models.EmbedContent).
type embedContentFunc func(ctx context.Context, model string, contents []*genai.Content, cfg *genai.EmbedContentConfig) (*genai.EmbedContentResponse, error)

// EmbeddingService generates query embeddings with the Google Gen AI SDK
// (google.golang.org/genai) against Vertex AI (Issue #11, ADR-001).
type EmbeddingService struct {
	model     string
	dimension int32
	location  string
	embed     embedContentFunc
}

// NewEmbeddingService creates a Vertex AI-backed Gen AI client.
// Gemini 3.x / gemini-embedding-2 are served from the "global" location
// (not us-central1), so GENAI_LOCATION is independent of the Cloud Run region.
func NewEmbeddingService(ctx context.Context, cfg *config.Config) (*EmbeddingService, error) {
	if cfg.ProjectID == "" {
		return nil, errors.New("embedding service: PROJECT_ID is required")
	}
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		Backend:  genai.BackendVertexAI,
		Project:  cfg.ProjectID,
		Location: cfg.GenAILocation,
	})
	if err != nil {
		return nil, fmt.Errorf("embedding service: create genai client: %w", err)
	}
	return newEmbeddingServiceWith(cfg, client.Models.EmbedContent), nil
}

func newEmbeddingServiceWith(cfg *config.Config, fn embedContentFunc) *EmbeddingService {
	return &EmbeddingService{
		model:     cfg.EmbeddingModelName,
		dimension: int32(cfg.GeminiEmbeddingDimension),
		location:  cfg.GenAILocation,
		embed:     fn,
	}
}

// Model returns the configured embedding model ID.
func (s *EmbeddingService) Model() string { return s.model }

// EmbedQuery returns a RETRIEVAL_QUERY embedding with the configured output dimensionality.
func (s *EmbeddingService) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	return s.embedText(ctx, text, "RETRIEVAL_QUERY")
}

// EmbedDocument returns a RETRIEVAL_DOCUMENT embedding (used to backfill embedding_v2).
func (s *EmbeddingService) EmbedDocument(ctx context.Context, text string) ([]float32, error) {
	return s.embedText(ctx, text, "RETRIEVAL_DOCUMENT")
}

func (s *EmbeddingService) embedText(ctx context.Context, text, taskType string) ([]float32, error) {
	start := time.Now()
	res, err := s.embed(ctx, s.model, genai.Text(text), &genai.EmbedContentConfig{
		TaskType:             taskType,
		OutputDimensionality: genai.Ptr(s.dimension),
	})
	if err != nil {
		return nil, fmt.Errorf("embed %s (%s @ %s): %w", taskType, s.model, s.location, err)
	}
	if res == nil || len(res.Embeddings) == 0 || res.Embeddings[0] == nil || len(res.Embeddings[0].Values) == 0 {
		return nil, fmt.Errorf("embed %s (%s): empty embedding in response", taskType, s.model)
	}
	vals := res.Embeddings[0].Values
	if int32(len(vals)) != s.dimension {
		return nil, fmt.Errorf("embed %s (%s): got %d dims, want %d", taskType, s.model, len(vals), s.dimension)
	}
	log.Printf("embedding: %s generated %d-d %s vector in %s", s.model, len(vals), taskType, time.Since(start))
	return vals, nil
}
