// Command backfill-embeddings populates products.embedding_v2 with
// gemini-embedding-2 (RETRIEVAL_DOCUMENT, 1024 dims) via the Google Gen AI SDK.
// Run by @deployer after migration 011 and before setting ACTIVE_EMBEDDING_VERSION=v2
// (ADR-001 Blue/Green, Issue #11). Rows that already have embedding_v2 are skipped
// unless -force is given.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/services"
)

func main() {
	force := flag.Bool("force", false, "re-embed rows that already have embedding_v2")
	dryRun := flag.Bool("dry-run", false, "embed but do not write to Spanner")
	flag.Parse()

	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	emb, err := services.NewEmbeddingService(ctx, cfg)
	if err != nil {
		log.Fatalf("embedding service: %v", err)
	}
	db := fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.ProjectID, cfg.SpannerInstanceID, cfg.SpannerDatabaseID)
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		log.Fatalf("spanner: %v", err)
	}
	defer client.Close()

	where := "WHERE embedding_v2 IS NULL"
	if *force {
		where = ""
	}
	stmt := spanner.Statement{SQL: `SELECT product_id, IFNULL(title, ''), IFNULL(description, ''), IFNULL(category, ''), IFNULL(brand, '')
		FROM products ` + where}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var muts []*spanner.Mutation
	n := 0
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("query: %v", err)
		}
		var id, title, desc, cat, brand string
		if err := row.Columns(&id, &title, &desc, &cat, &brand); err != nil {
			log.Fatalf("scan: %v", err)
		}
		text := strings.TrimSpace(strings.Join([]string{title, desc, cat, brand}, " "))
		vec, err := emb.EmbedDocument(ctx, text)
		if err != nil {
			log.Fatalf("embed %s: %v", id, err)
		}
		muts = append(muts, spanner.Update("products", []string{"product_id", "embedding_v2"}, []interface{}{id, vec}))
		n++
	}
	log.Printf("embedded %d products with %s (%d dims)", n, emb.Model(), cfg.GeminiEmbeddingDimension)
	if *dryRun || n == 0 {
		return
	}
	if _, err := client.Apply(ctx, muts); err != nil {
		log.Fatalf("apply: %v", err)
	}
	log.Printf("wrote embedding_v2 for %d products", n)
}
