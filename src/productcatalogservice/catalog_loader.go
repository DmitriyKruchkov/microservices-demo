// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"fmt"

	"os"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	pb "github.com/GoogleCloudPlatform/microservices-demo/src/productcatalogservice/genproto"
	"github.com/golang/protobuf/jsonpb"
	"github.com/jackc/pgx/v5/pgxpool"
)

func loadCatalog(catalog *pb.ListProductsResponse) error {
	catalogMutex.Lock()
	defer catalogMutex.Unlock()

	if os.Getenv("ALLOYDB_CLUSTER_NAME") != "" {
		return loadCatalogFromAlloyDB(catalog)
	}

	return loadCatalogFromLocalFile(catalog)
}

func loadCatalogFromLocalFile(catalog *pb.ListProductsResponse) error {
	log.Info("loading catalog from local products.json file...")

	catalogJSON, err := os.ReadFile("products.json")
	if err != nil {
		log.Warnf("failed to open product catalog json file: %v", err)
		return err
	}

	if err := jsonpb.Unmarshal(bytes.NewReader(catalogJSON), catalog); err != nil {
		log.Warnf("failed to parse the catalog JSON: %v", err)
		return err
	}

	log.Info("successfully parsed product catalog json")
	return nil
}

func getSecretPayload(project, secret, version string) (string, error) {
	ctx := context.Background()
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Warnf("failed to create SecretManager client: %v", err)
		return "", err
	}
	defer client.Close()

	req := &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/%s", project, secret, version),
	}

	// Call the API.
	result, err := client.AccessSecretVersion(ctx, req)
	if err != nil {
		log.Warnf("failed to access SecretVersion: %v", err)
		return "", err
	}

	return string(result.Payload.Data), nil
}

func loadCatalogFromAlloyDB(catalog *pb.ListProductsResponse) error {
	
	pgUser := os.Getenv("pgUser")
	pgPassword := os.Getenv("pgPassword")
	pgHost := os.Getenv("pgHost")
	pgPort := os.Getenv("pgPort")
	pgDatabaseName := os.Getenv("pgDatabaseName")
	pgTableName := os.Getenv("pgTableName")

	dsn := fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=disable",
		pgUser, pgPassword, pgHost, pgPort, pgDatabaseName,
	)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		log.Warnf("failed to parse DSN config: %v", err)
		return err
	}

	// (необязательно) немного настроек пула
	// cfg.MaxConns = 10

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Warnf("failed to set-up pgx pool: %v", err)
		return err
	}
	defer pool.Close()

	query := `
SELECT id, name, description, picture,
       price_usd_currency_code, price_usd_units, price_usd_nanos,
       categories
FROM ` + pgTableName

	rows, err := pool.Query(ctx, query)
	if err != nil {
		log.Warnf("failed to query database: %v", err)
		return err
	}
	defer rows.Close()

	catalog.Products = catalog.Products[:0]

	for rows.Next() {
		product := &pb.Product{PriceUsd: &pb.Money{}}

		// в БД categories хранится как CSV-строка
		var categoriesCSV string

		if err := rows.Scan(
			&product.Id,
			&product.Name,
			&product.Description,
			&product.Picture,
			&product.PriceUsd.CurrencyCode,
			&product.PriceUsd.Units,
			&product.PriceUsd.Nanos,
			&categoriesCSV,
		); err != nil {
			log.Warnf("failed to scan query result row: %v", err)
			return err
		}

		categoriesCSV = strings.ToLower(strings.TrimSpace(categoriesCSV))
		if categoriesCSV == "" {
			product.Categories = nil
		} else {
			product.Categories = strings.Split(categoriesCSV, ",")
			for i := range product.Categories {
				product.Categories[i] = strings.TrimSpace(product.Categories[i])
			}
		}

		catalog.Products = append(catalog.Products, product)
	}

	if err := rows.Err(); err != nil {
		log.Warnf("rows iteration error: %v", err)
		return err
	}

	log.Info("successfully parsed product catalog from PostgreSQL")
	return nil
}
