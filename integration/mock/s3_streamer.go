package mock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
)

// Stream provides a simplified implementation of s3reader.Stream for testing purposes
// It directly reads from the Files map rather than using the S3 manager
func (m *S3Client) Stream(ctx context.Context, bucket, key string, offset int64, fn func([]byte, int64) error) error {
	// If key contains bucket in the beginning, strip it
	if strings.HasPrefix(key, bucket+"/") {
		key = strings.TrimPrefix(key, bucket+"/")
	}

	bucketKey := fmt.Sprintf("%s/%s", bucket, key)

	content, ok := m.Files[bucketKey]
	if !ok {
		// Try finding by suffix match if exact match fails
		for k, v := range m.Files {
			if strings.HasSuffix(k, key) {
				content = v
				ok = true
				break
			}
		}

		if !ok {
			return fmt.Errorf("mock S3: key not found: %s", bucketKey)
		}
	}

	// Read content line by line
	scanner := bufio.NewScanner(bytes.NewReader(content))
	scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB buffer

	lineNum := int64(0)
	for scanner.Scan() {
		// Skip lines before offset
		if lineNum < offset {
			lineNum++
			continue
		}

		// Process the line
		if err := fn(scanner.Bytes(), lineNum); err != nil {
			return err
		}

		lineNum++

		// Check context cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Continue processing
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error scanning lines: %w", err)
	}

	return nil
}
