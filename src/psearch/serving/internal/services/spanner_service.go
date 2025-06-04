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
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
)

// SpannerService handles interactions with Spanner database
type SpannerService struct {
	client     *spanner.Client
	config     *config.Config
	embeddings *EmbeddingService
}

// NewSpannerService creates a new Spanner service
func NewSpannerService(ctx context.Context, cfg *config.Config, embeddings *EmbeddingService) (*SpannerService, error) {
	// Create the Spanner client
	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s", 
		cfg.ProjectID, cfg.SpannerInstanceID, cfg.SpannerDatabaseID)
	
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}

	return &SpannerService{
		client:     client,
		config:     cfg,
		embeddings: embeddings,
	}, nil
}

// Close closes the Spanner client connection
func (s *SpannerService) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

// GetProduct retrieves a single product by ID
func (s *SpannerService) GetProduct(ctx context.Context, productID string) (map[string]interface{}, error) {
	row, err := s.client.Single().ReadRow(ctx, "products", spanner.Key{productID}, []string{"product_data"})
	if err != nil {
		return nil, fmt.Errorf("failed to read product %s: %v", productID, err)
	}

	var productDataJSON string
	if err := row.Column(0, &productDataJSON); err != nil {
		return nil, fmt.Errorf("failed to scan product data: %v", err)
	}

	var productData map[string]interface{}
	if err := json.Unmarshal([]byte(productDataJSON), &productData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal product data: %v", err)
	}

	return productData, nil
}

// GetProductsBatch retrieves multiple products by their IDs in a single batch
func (s *SpannerService) GetProductsBatch(ctx context.Context, productIDs []string) (map[string]map[string]interface{}, error) {
	if len(productIDs) == 0 {
		return make(map[string]map[string]interface{}), nil
	}

	startTime := time.Now()

	// Create a SQL statement with UNNEST to handle large number of product IDs
	stmt := spanner.Statement{
		SQL: `SELECT product_id, product_data 
              FROM products 
              WHERE product_id IN UNNEST(@product_ids)`,
		Params: map[string]interface{}{
			"product_ids": productIDs,
		},
	}

	resultMap := make(map[string]map[string]interface{})
	
	// Execute the query
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating through query results: %v", err)
		}

		var productID string
		var productDataJSON spanner.NullJSON

		if err := row.Columns(&productID, &productDataJSON); err != nil {
			return nil, fmt.Errorf("failed to scan columns: %v", err)
		}

		if productDataJSON.Valid {
			// Type assert productDataJSON.Value directly to map[string]interface{}
			productData, ok := productDataJSON.Value.(map[string]interface{})
			if !ok {
				// Log the actual type if the assertion fails
				log.Printf("DEBUG: Unexpected type for productDataJSON.Value: %T", productDataJSON.Value)
				return nil, fmt.Errorf("failed to type assert product data from NullJSON.Value")
			}
			resultMap[productID] = productData
		}
	}

	elapsed := time.Since(startTime)
	log.Printf("Spanner batch fetch for %d products took %s, retrieved %d", 
		len(productIDs), elapsed, len(resultMap))

	return resultMap, nil
}

// HybridSearch performs a hybrid search using both vector similarity and text search
func (s *SpannerService) HybridSearch(ctx context.Context, query string, limit int, minScore float64, alpha float64) ([]models.SearchResult, error) {
	startTime := time.Now()

	// Generate embeddings for the query
	embedding, err := s.embeddings.GenerateEmbedding(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %v", err)
	}

	// Construct hybrid search SQL query
	// This combines vector similarity search with text search using the configured alpha value
	sql := `
		@{optimizer_version=7}
		WITH ann AS (
		SELECT offset + 1 AS rank, product_id, title, product_data
		FROM UNNEST(ARRAY(
			SELECT AS STRUCT product_id, title, product_data
			FROM products @{FORCE_INDEX=products_by_embedding}
			WHERE embedding IS NOT NULL
			ORDER BY APPROX_COSINE_DISTANCE(embedding, @query_embedding,
			OPTIONS=>JSON'{"num_leaves_to_search": 10}')
			LIMIT @limit)) WITH OFFSET AS offset
		),
		fts AS (
		SELECT offset + 1 AS rank, product_id, title, product_data
		FROM UNNEST(ARRAY(
			SELECT AS STRUCT product_id, title, product_data
			FROM products
			WHERE SEARCH(title_tokens, @query_text)
			ORDER BY SCORE(title_tokens, @query_text) DESC
			LIMIT @limit)) WITH OFFSET AS offset
		)
		SELECT 
			SUM(1 / (60 + rank)) AS rrf_score, 
			product_id,
			ANY_VALUE(title) AS title,
			ANY_VALUE(product_data) AS product_data 
		FROM ((
		SELECT rank, product_id, title, product_data
		FROM ann
		)
		UNION ALL (
		SELECT rank, product_id, title, product_data
		FROM fts
		))
		GROUP BY product_id
		ORDER BY rrf_score DESC
		LIMIT @limit;
	`

	// Create parameters
	params := map[string]interface{}{
		"query_embedding": embedding,
		"query_text":      query,
		"limit":           limit,
	}

	// Execute the query
	stmt := spanner.Statement{SQL: sql, Params: params}
	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var results []models.SearchResult
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error iterating through search results: %v", err)
		}

		var productIDInt string
		var title string
		var productDataJSON spanner.NullJSON
		var hybridScore float64

		if err := row.Columns(&hybridScore, &productIDInt, &title, &productDataJSON); err != nil {
			return nil, fmt.Errorf("failed to scan search result: %v", err)
		}

		productID := fmt.Sprintf("%d", productIDInt)

		if !productDataJSON.Valid {
			continue
		}

		// Type assert productDataJSON.Value directly to map[string]interface{}
		productData, ok := productDataJSON.Value.(map[string]interface{})
		if !ok {
			// Log the actual type if the assertion fails
			log.Printf("DEBUG: Unexpected type for productDataJSON.Value in search result: %T", productDataJSON.Value)
			return nil, fmt.Errorf("failed to type assert product data from NullJSON.Value for search result")
		}

		// Skip if score is below minimum threshold
		if hybridScore < minScore {
			continue
		}

		// Transform to search result
		searchResult, err := s.transformToSearchResult(productID, productData, hybridScore)
		if err != nil {
			log.Printf("Warning: could not transform product %s: %v", productID, err)
			continue
		}

		results = append(results, searchResult)
	}

	elapsed := time.Since(startTime)
	log.Printf("Hybrid search completed in %s, found %d results", elapsed, len(results))

	return results, nil
}

// transformToSearchResult converts product data into a SearchResult
func (s *SpannerService) transformToSearchResult(productID string, productData map[string]interface{}, score float64) (models.SearchResult, error) {
	// Create score map
	scoreMap := map[string]float64{
		"hybrid": score,
	}

	// Extract name
	name, _ := productData["name"].(string)
	
	// Extract title
	title, _ := productData["title"].(string)

	// Extract brands
	var brands []string
	if brandsData, ok := productData["brands"].([]interface{}); ok {
		for _, b := range brandsData {
			if brand, ok := b.(string); ok {
				brands = append(brands, brand)
			}
		}
	}

	// Extract categories
	var categories []string
	if categoriesData, ok := productData["categories"].([]interface{}); ok {
		for _, c := range categoriesData {
			if category, ok := c.(string); ok {
				categories = append(categories, category)
			}
		}
	}

	// Handle price info
	priceInfo := models.PriceInfo{
		CurrencyCode: "USD", // Default
	}
	if priceInfoData, ok := productData["priceInfo"].(map[string]interface{}); ok {
		if cost, ok := priceInfoData["cost"].(string); ok {
			priceInfo.Cost = cost
		}
		if currencyCode, ok := priceInfoData["currencyCode"].(string); ok {
			priceInfo.CurrencyCode = currencyCode
		}
		if originalPrice, ok := priceInfoData["originalPrice"].(string); ok {
			priceInfo.OriginalPrice = originalPrice
		}
		if price, ok := priceInfoData["price"].(string); ok {
			priceInfo.Price = price
		}
		if effectiveTime, ok := priceInfoData["priceEffectiveTime"].(string); ok {
			priceInfo.PriceEffectiveTime = effectiveTime
		}
		if expireTime, ok := priceInfoData["priceExpireTime"].(string); ok {
			priceInfo.PriceExpireTime = expireTime
		}
	}

	// Handle images
	var images []models.Image
	if imagesData, ok := productData["images"].([]interface{}); ok {
		for _, img := range imagesData {
			if imgMap, ok := img.(map[string]interface{}); ok {
				height := "0"
				width := "0"
				uri := ""

				if h, ok := imgMap["height"].(string); ok {
					height = h
				}
				if w, ok := imgMap["width"].(string); ok {
					width = w
				}
				if u, ok := imgMap["uri"].(string); ok {
					uri = u
				}

				// Convert gs:// URLs to https://storage.googleapis.com/
				if len(uri) > 5 && uri[:5] == "gs://" {
					uri = "https://storage.googleapis.com/" + uri[5:]
				}

				images = append(images, models.Image{
					Height: height,
					Width:  width,
					URI:    uri,
				})
			}
		}
	}

	// Extract sizes
	var sizes []string
	if sizesData, ok := productData["sizes"].([]interface{}); ok {
		for _, s := range sizesData {
			if size, ok := s.(string); ok {
				sizes = append(sizes, size)
			} else if sizeNum, ok := s.(float64); ok {
				sizes = append(sizes, fmt.Sprintf("%v", sizeNum))
			}
		}
	}

	// Extract URI
	uri, _ := productData["uri"].(string)

	// Process attributes
	var attributes []models.Attribute
	if attrsData, ok := productData["attributes"].([]interface{}); ok {
		for _, attr := range attrsData {
			if attrMap, ok := attr.(map[string]interface{}); ok {
				key, keyOk := attrMap["key"].(string)
				valueData, valueOk := attrMap["value"].(map[string]interface{})
				
				if keyOk && valueOk {
					// Handle attribute value
					attrValue := models.AttributeValue{}
					
					if textData, ok := valueData["text"].([]interface{}); ok {
						for _, t := range textData {
							if textStr, ok := t.(string); ok {
								attrValue.Text = append(attrValue.Text, textStr)
							}
						}
					}

					if numbersData, ok := valueData["numbers"].([]interface{}); ok {
						for _, n := range numbersData {
							if num, ok := n.(float64); ok {
								attrValue.Numbers = append(attrValue.Numbers, num)
							}
						}
					}

					if indexable, ok := valueData["indexable"].(string); ok {
						attrValue.Indexable = &indexable
					}

					if searchable, ok := valueData["searchable"].(string); ok {
						attrValue.Searchable = &searchable
					}

					attributes = append(attributes, models.Attribute{
						Key:   key,
						Value: attrValue,
					})
				}
			}
		}
	}

	// Handle tags as attributes
	if tagsData, ok := productData["tags"].([]interface{}); ok {
		for _, t := range tagsData {
			if tag, ok := t.(string); ok {
				attributes = append(attributes, models.Attribute{
					Key: "tag",
					Value: models.AttributeValue{
						Text: []string{tag},
					},
				})
			}
		}
	}

	// Create search result
	result := models.SearchResult{
		ID:                productID,
		Name:              name,
		Title:             title,
		Brands:            brands,
		Categories:        categories,
		PriceInfo:         priceInfo,
		Availability:      "IN_STOCK", // Default
		Images:            images,
		Sizes:             sizes,
		RetrievableFields: "*",
		Attributes:        attributes,
		URI:               uri,
		Score:             scoreMap,
	}

	return result, nil
}
