# DynamoDB Data Generator

A command-line tool for generating and populating DynamoDB tables with random test data. The tool can either create a new table or add data to an existing table.

## Features

- Create new DynamoDB tables with Point-in-Time Recovery (PITR) enabled
- Generate random test data with various DynamoDB attribute types:
  - String, Number, Boolean, Null
  - String Set, Number Set, Binary Set
  - Binary
  - Map
  - List
- Add data to existing tables
- Progress reporting during data generation
- Unique item IDs to prevent conflicts

## Prerequisites

- Go 1.16 or later
- AWS credentials configured (via environment variables, AWS CLI, or IAM role)
- Appropriate AWS permissions to create/modify DynamoDB tables

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/ddb-pitr.git
cd ddb-pitr

# Build the tool
go build -o ddb-datagen cmd/ddb-datagen/main.go
```

## Usage

### Create a New Table and Add Data

```bash
./ddb-datagen -items 1000
```

This will:
1. Create a new table with a random name (prefixed with "ddb-datagen-")
2. Enable Point-in-Time Recovery
3. Generate and add 1000 random items

### Add Data to an Existing Table

```bash
./ddb-datagen -table my-existing-table -items 1000
```

This will:
1. Verify the existing table is accessible
2. Generate and add 1000 random items to the table

## Command Line Options

- `-items`: Number of items to generate (default: 100)
- `-table`: Name of an existing table to use (if not provided, a new table will be created)

## Table Structure

The tool creates tables with the following structure:

- Partition Key (PK): String
- Sort Key (SK): String
- Additional random attributes of various types

## Generated Data

Each item includes:
- A unique ID in the PK field
- "METADATA" in the SK field
- 5-15 random attributes of various types:
  - Basic types (String, Number, Boolean, Null)
  - Collection types (String Set, Number Set, Binary Set)
  - Complex types (Map, List)
  - Binary data

## Example Output

```
Created table: ddb-datagen-a1b2c3d4
Waiting for table to become active...
Enabling Point-in-Time Recovery...
PITR enabled successfully
Generating 1000 items...
Written 10 items...
Written 20 items...
...
Successfully populated table ddb-datagen-a1b2c3d4
Items added: 1000
```

## Error Handling

The tool will:
- Fail if it cannot create a new table
- Fail if it cannot access an existing table
- Continue processing if individual item writes fail
- Report any write failures in the output

