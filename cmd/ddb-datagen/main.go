// Package main provides a data generator for DynamoDB tables.
// It supports creating tables with GSI/LSI, populating with test data,
// and performing lifecycle operations (UPDATE/DELETE) for PITR verification.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const tableNamePrefix = "ddb-datagen-"

// DataGenerator defines operations needed for data generation.
// The AWS DynamoDB client satisfies this interface.
type DataGenerator interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	CreateTable(ctx context.Context, params *dynamodb.CreateTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.CreateTableOutput, error)
	DescribeTable(ctx context.Context, params *dynamodb.DescribeTableInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DescribeTableOutput, error)
	UpdateContinuousBackups(ctx context.Context, params *dynamodb.UpdateContinuousBackupsInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateContinuousBackupsOutput, error)
}

// Compile-time check that dynamodb.Client satisfies DataGenerator
var _ DataGenerator = (*dynamodb.Client)(nil)

// Config holds the command-line configuration for the data generator.
type Config struct {
	TableName   string
	NumItems    int
	Mode        string // "put" or "lifecycle"
	UpdateCount int
	DeleteCount int
	Seed        int64
	EnableGSI   bool
	EnableLSI   bool
}

func randomString(r *rand.Rand, n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[r.Intn(len(letters))]
	}
	return string(b)
}

func randomNumber(r *rand.Rand, min, max int) int {
	return min + r.Intn(max-min+1)
}

func randomAttributeNames(r *rand.Rand, count int) []string {
	prefixes := []string{"Attr", "Field", "Data", "Value", "Info", "Meta", "Config", "Setting"}
	suffixes := []string{"Name", "Type", "Status", "Count", "Size", "Level", "Score", "Index"}

	names := make([]string, count)
	used := make(map[string]bool)

	for i := 0; i < count; i++ {
		var name string
		for {
			prefix := prefixes[r.Intn(len(prefixes))]
			suffix := suffixes[r.Intn(len(suffixes))]
			name = fmt.Sprintf("%s%s", prefix, suffix)
			if !used[name] {
				used[name] = true
				break
			}
		}
		names[i] = name
	}
	return names
}

func generateUniqueStrings(r *rand.Rand, n, minLen, maxLen int) []string {
	strs := make([]string, 0, n)
	used := make(map[string]bool)

	for len(strs) < n {
		str := randomString(r, randomNumber(r, minLen, maxLen))
		if !used[str] {
			used[str] = true
			strs = append(strs, str)
		}
	}
	return strs
}

func generateUniqueNumbers(r *rand.Rand, n, min, max int) []string {
	numbers := make([]string, 0, n)
	used := make(map[string]bool)

	for len(numbers) < n {
		num := fmt.Sprintf("%d", randomNumber(r, min, max))
		if !used[num] {
			used[num] = true
			numbers = append(numbers, num)
		}
	}
	return numbers
}

func generateUniqueBinaries(r *rand.Rand, n, minLen, maxLen int) [][]byte {
	binaries := make([][]byte, 0, n)
	used := make(map[string]bool)

	for len(binaries) < n {
		binary := []byte(randomString(r, randomNumber(r, minLen, maxLen)))
		strBinary := string(binary)
		if !used[strBinary] {
			used[strBinary] = true
			binaries = append(binaries, binary)
		}
	}
	return binaries
}

// generateRandomItem creates a random DynamoDB item with various attribute types.
// When enableGSI or enableLSI is true, adds the required index attributes.
func generateRandomItem(r *rand.Rand, id int, enableGSI, enableLSI bool) map[string]types.AttributeValue {
	numAttributes := randomNumber(r, 5, 15)
	attributeNames := randomAttributeNames(r, numAttributes)

	now := time.Now().UnixMilli()

	// Base item with primary key
	item := map[string]types.AttributeValue{
		"PK": &types.AttributeValueMemberS{Value: fmt.Sprintf("ITEM#%d", id)},
		"SK": &types.AttributeValueMemberS{Value: "METADATA"},
	}

	// LSI attribute: timestamp (same PK, different sort key)
	if enableLSI {
		item["timestamp"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now+int64(id))}
	}

	// GSI attributes: category (PK) and createdAt (SK)
	if enableGSI {
		item["category"] = &types.AttributeValueMemberS{Value: fmt.Sprintf("cat-%d", id%5)}
		item["createdAt"] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", now+int64(id))}
	}

	// Add random attributes
	for _, name := range attributeNames {
		switch r.Intn(10) {
		case 0: // String
			item[name] = &types.AttributeValueMemberS{Value: randomString(r, randomNumber(r, 5, 50))}
		case 1: // Number
			item[name] = &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", randomNumber(r, 1, 1000))}
		case 2: // Boolean
			item[name] = &types.AttributeValueMemberBOOL{Value: r.Float32() > 0.5}
		case 3: // Null
			item[name] = &types.AttributeValueMemberNULL{Value: true}
		case 4: // String Set
			numStrings := randomNumber(r, 1, 5)
			item[name] = &types.AttributeValueMemberSS{Value: generateUniqueStrings(r, numStrings, 5, 20)}
		case 5: // Number Set
			numNumbers := randomNumber(r, 1, 5)
			item[name] = &types.AttributeValueMemberNS{Value: generateUniqueNumbers(r, numNumbers, 1, 1000)}
		case 6: // Binary
			item[name] = &types.AttributeValueMemberB{Value: []byte(randomString(r, randomNumber(r, 5, 20)))}
		case 7: // Binary Set
			numBinaries := randomNumber(r, 1, 3)
			item[name] = &types.AttributeValueMemberBS{Value: generateUniqueBinaries(r, numBinaries, 5, 10)}
		case 8: // Map
			numMapItems := randomNumber(r, 2, 5)
			mapItems := make(map[string]types.AttributeValue)
			usedKeys := make(map[string]bool)
			for i := 0; i < numMapItems; i++ {
				var key string
				for {
					key = randomString(r, 5)
					if !usedKeys[key] {
						usedKeys[key] = true
						break
					}
				}
				mapItems[key] = &types.AttributeValueMemberS{Value: randomString(r, randomNumber(r, 5, 20))}
			}
			item[name] = &types.AttributeValueMemberM{Value: mapItems}
		case 9: // List
			numListItems := randomNumber(r, 1, 5)
			listItems := make([]types.AttributeValue, numListItems)
			for i := range listItems {
				listItems[i] = &types.AttributeValueMemberS{Value: randomString(r, randomNumber(r, 5, 20))}
			}
			item[name] = &types.AttributeValueMemberL{Value: listItems}
		}
	}

	return item
}

// createTableWithIndexes creates a DynamoDB table with optional GSI and LSI.
// Base schema: PK (string), SK (string)
// LSI "ByTimestamp": PK (string), timestamp (number)
// GSI "ByCategory": category (string), createdAt (number)
func createTableWithIndexes(ctx context.Context, client DataGenerator, tableName string, enableGSI, enableLSI bool) error {
	attrDefs := []types.AttributeDefinition{
		{AttributeName: aws.String("PK"), AttributeType: types.ScalarAttributeTypeS},
		{AttributeName: aws.String("SK"), AttributeType: types.ScalarAttributeTypeS},
	}

	keySchema := []types.KeySchemaElement{
		{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
		{AttributeName: aws.String("SK"), KeyType: types.KeyTypeRange},
	}

	input := &dynamodb.CreateTableInput{
		TableName:            aws.String(tableName),
		AttributeDefinitions: attrDefs,
		KeySchema:            keySchema,
		BillingMode:          types.BillingModePayPerRequest,
	}

	// Add LSI: ByTimestamp (same PK, timestamp as sort key)
	if enableLSI {
		input.AttributeDefinitions = append(input.AttributeDefinitions,
			types.AttributeDefinition{AttributeName: aws.String("timestamp"), AttributeType: types.ScalarAttributeTypeN})
		input.LocalSecondaryIndexes = []types.LocalSecondaryIndex{
			{
				IndexName: aws.String("ByTimestamp"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("PK"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("timestamp"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		}
	}

	// Add GSI: ByCategory (category as PK, createdAt as SK)
	if enableGSI {
		input.AttributeDefinitions = append(input.AttributeDefinitions,
			types.AttributeDefinition{AttributeName: aws.String("category"), AttributeType: types.ScalarAttributeTypeS},
			types.AttributeDefinition{AttributeName: aws.String("createdAt"), AttributeType: types.ScalarAttributeTypeN})
		input.GlobalSecondaryIndexes = []types.GlobalSecondaryIndex{
			{
				IndexName: aws.String("ByCategory"),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("category"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("createdAt"), KeyType: types.KeyTypeRange},
				},
				Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
			},
		}
	}

	_, err := client.CreateTable(ctx, input)
	return err
}

// runPutMode creates new items in the table.
func runPutMode(ctx context.Context, client DataGenerator, cfg Config, r *rand.Rand) error {
	fmt.Printf("Generating %d items...\n", cfg.NumItems)
	successCount := 0

	for i := 0; i < cfg.NumItems; i++ {
		item := generateRandomItem(r, i, cfg.EnableGSI, cfg.EnableLSI)
		_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(cfg.TableName),
			Item:      item,
		})
		if err != nil {
			log.Printf("Failed to write item %d: %v", i, err)
			continue
		}
		successCount++
		if (i+1)%10 == 0 {
			fmt.Printf("Written %d items...\n", i+1)
		}
	}

	fmt.Printf("Items added: %d\n", successCount)
	return nil
}

// runLifecycleMode performs UPDATE and DELETE operations on existing items.
// Uses the same seed to deterministically select which items to modify.
func runLifecycleMode(ctx context.Context, client DataGenerator, cfg Config, r *rand.Rand) error {
	// Advance the random state to match where put mode left off
	// This ensures lifecycle mode selects the same items regardless of when it's called
	for i := 0; i < cfg.NumItems; i++ {
		generateRandomItem(r, i, cfg.EnableGSI, cfg.EnableLSI)
	}

	fmt.Printf("Lifecycle mode: updating %d items, deleting %d items\n", cfg.UpdateCount, cfg.DeleteCount)

	// Perform updates on first N items
	updateSuccess := 0
	for i := 0; i < cfg.UpdateCount; i++ {
		pk := fmt.Sprintf("ITEM#%d", i)
		sk := "METADATA"

		_, err := client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(cfg.TableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: pk},
				"SK": &types.AttributeValueMemberS{Value: sk},
			},
			UpdateExpression: aws.String("SET #data = :val, updatedAt = :ts"),
			ExpressionAttributeNames: map[string]string{
				"#data": "data",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":val": &types.AttributeValueMemberS{Value: fmt.Sprintf("updated-%d", i)},
				":ts":  &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", time.Now().UnixMilli())},
			},
		})
		if err != nil {
			log.Printf("Failed to update item %d: %v", i, err)
			continue
		}
		updateSuccess++
	}
	fmt.Printf("Items updated: %d\n", updateSuccess)

	// Delete last M items (from end of range to avoid overlap with updates)
	deleteSuccess := 0
	startDelete := cfg.NumItems - cfg.DeleteCount
	for i := startDelete; i < cfg.NumItems; i++ {
		pk := fmt.Sprintf("ITEM#%d", i)
		sk := "METADATA"

		_, err := client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(cfg.TableName),
			Key: map[string]types.AttributeValue{
				"PK": &types.AttributeValueMemberS{Value: pk},
				"SK": &types.AttributeValueMemberS{Value: sk},
			},
		})
		if err != nil {
			log.Printf("Failed to delete item %d: %v", i, err)
			continue
		}
		deleteSuccess++
	}
	fmt.Printf("Items deleted: %d\n", deleteSuccess)

	return nil
}

func main() {
	cfg := Config{}

	flag.StringVar(&cfg.TableName, "table", "", "Table name (creates new if empty)")
	flag.IntVar(&cfg.NumItems, "items", 100, "Number of items (for put mode or reference for lifecycle)")
	flag.StringVar(&cfg.Mode, "mode", "put", "Operation mode: put | lifecycle")
	flag.IntVar(&cfg.UpdateCount, "update-count", 0, "Items to update (lifecycle mode)")
	flag.IntVar(&cfg.DeleteCount, "delete-count", 0, "Items to delete (lifecycle mode)")
	flag.Int64Var(&cfg.Seed, "seed", 0, "Random seed (0 = time-based)")
	flag.BoolVar(&cfg.EnableGSI, "gsi", false, "Create table with GSI (ByCategory)")
	flag.BoolVar(&cfg.EnableLSI, "lsi", false, "Create table with LSI (ByTimestamp)")
	flag.Parse()

	// Initialize random source
	var seed int64
	if cfg.Seed == 0 {
		seed = time.Now().UnixNano()
	} else {
		seed = cfg.Seed
	}
	r := rand.New(rand.NewSource(seed))
	fmt.Printf("Using seed: %d\n", seed)

	// Load AWS configuration
	awsCfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Unable to load SDK config: %v", err)
	}

	client := dynamodb.NewFromConfig(awsCfg)
	ctx := context.Background()

	// Handle table creation or validation
	if cfg.TableName == "" {
		cfg.TableName = tableNamePrefix + randomString(r, 8)

		if err := createTableWithIndexes(ctx, client, cfg.TableName, cfg.EnableGSI, cfg.EnableLSI); err != nil {
			log.Fatalf("Failed to create table: %v", err)
		}
		fmt.Printf("Created table: %s\n", cfg.TableName)

		// Wait for table to become active
		fmt.Println("Waiting for table to become active...")
		waiter := dynamodb.NewTableExistsWaiter(client)
		if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(cfg.TableName),
		}, 300*time.Second); err != nil {
			log.Fatalf("Failed to wait for table: %v", err)
		}

		// Enable PITR
		fmt.Println("Enabling Point-in-Time Recovery...")
		_, err = client.UpdateContinuousBackups(ctx, &dynamodb.UpdateContinuousBackupsInput{
			TableName: aws.String(cfg.TableName),
			PointInTimeRecoverySpecification: &types.PointInTimeRecoverySpecification{
				PointInTimeRecoveryEnabled: aws.Bool(true),
			},
		})
		if err != nil {
			log.Printf("Warning: Failed to enable PITR: %v", err)
		} else {
			fmt.Println("PITR enabled successfully")
		}
	} else {
		// Verify table exists
		_, err := client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
			TableName: aws.String(cfg.TableName),
		})
		if err != nil {
			log.Fatalf("Failed to access table %s: %v", cfg.TableName, err)
		}
		fmt.Printf("Using existing table: %s\n", cfg.TableName)
	}

	// Run the appropriate mode
	switch cfg.Mode {
	case "put":
		if err := runPutMode(ctx, client, cfg, r); err != nil {
			log.Fatalf("Put mode failed: %v", err)
		}
	case "lifecycle":
		if err := runLifecycleMode(ctx, client, cfg, r); err != nil {
			log.Fatalf("Lifecycle mode failed: %v", err)
		}
	default:
		log.Fatalf("Unknown mode: %s (use 'put' or 'lifecycle')", cfg.Mode)
	}

	fmt.Printf("\nTable: %s\n", cfg.TableName)
}
