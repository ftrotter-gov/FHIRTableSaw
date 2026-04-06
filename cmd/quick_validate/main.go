package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// ResourceType extracts the resourceType field from JSON
type ResourceType struct {
	ResourceType string `json:"resourceType"`
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <input.ndjson|input.ndjson.gz|->\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\nValidates FHIR v4 resources in NDJSON file and reports validation statistics.\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s practitioners.ndjson\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s practitioners.ndjson.gz\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  cat practitioners.ndjson | %s -\n", os.Args[0])
		os.Exit(1)
	}

	inputFile := os.Args[1]
	
	// Open input
	var reader io.Reader
	var fileSize int64
	
	if inputFile == "-" {
		reader = os.Stdin
		fileSize = -1 // Unknown size for stdin
	} else {
		file, err := os.Open(inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
		
		// Get file size for progress reporting
		stat, err := file.Stat()
		if err == nil {
			fileSize = stat.Size()
		}
		
		// Handle gzip if needed
		if strings.HasSuffix(inputFile, ".gz") {
			gzReader, err := gzip.NewReader(file)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error creating gzip reader: %v\n", err)
				os.Exit(1)
			}
			defer gzReader.Close()
			reader = gzReader
		} else {
			reader = file
		}
	}
	
	// Process the NDJSON file
	stats := processNDJSON(reader, fileSize)
	
	// Print results
	printResults(stats, inputFile)
}

// ValidationStats tracks validation statistics
type ValidationStats struct {
	TotalRecords    int64
	ValidRecords    int64
	InvalidRecords  int64
	ParseErrors     int64
	ResourceCounts  map[string]int64
	StartTime       time.Time
	EndTime         time.Time
}

func processNDJSON(reader io.Reader, fileSize int64) *ValidationStats {
	stats := &ValidationStats{
		ResourceCounts: make(map[string]int64),
		StartTime:      time.Time(time.Now()),
	}
	
	scanner := bufio.NewScanner(reader)
	// Set a larger buffer for big lines (up to 50MB per line)
	const maxCapacity = 50 * 1024 * 1024
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)
	
	lineNum := 0
	lastProgressUpdate := time.Now()
	
	for scanner.Scan() {
		lineNum++
		stats.TotalRecords++
		
		// Show progress every second
		if time.Since(lastProgressUpdate) > time.Second {
			fmt.Fprintf(os.Stderr, "\rProcessing: %d records...", stats.TotalRecords)
			lastProgressUpdate = time.Now()
		}
		
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		
		// First, try to parse as generic JSON to extract resourceType
		var resource ResourceType
		if err := json.Unmarshal(line, &resource); err != nil {
			stats.ParseErrors++
			stats.InvalidRecords++
			fmt.Fprintf(os.Stderr, "\nLine %d: JSON parse error: %v\n", lineNum, err)
			continue
		}
		
		// Track resource type
		resourceType := resource.ResourceType
		if resourceType == "" {
			resourceType = "Unknown"
		}
		stats.ResourceCounts[resourceType]++
		
		// Validate the FHIR resource
		isValid := validateFHIRResource(line, resourceType, lineNum)
		
		if isValid {
			stats.ValidRecords++
		} else {
			stats.InvalidRecords++
		}
	}
	
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "\nError reading input: %v\n", err)
	}
	
	stats.EndTime = time.Now()
	fmt.Fprintf(os.Stderr, "\r") // Clear progress line
	
	return stats
}

// validateFHIRResource performs fast FHIR v4 validation
// This is a practical validator focusing on common FHIR requirements
func validateFHIRResource(jsonData []byte, resourceType string, lineNum int) bool {
	// Parse as generic map to perform validation checks
	var resource map[string]interface{}
	if err := json.Unmarshal(jsonData, &resource); err != nil {
		return false
	}
	
	// Check required fields for FHIR resources
	if resource["resourceType"] == nil {
		return false
	}
	
	// Perform resource-specific validation based on FHIR R4 requirements
	switch resourceType {
	case "Patient":
		return validatePatient(resource, lineNum)
	case "Practitioner":
		return validatePractitioner(resource, lineNum)
	case "PractitionerRole":
		return validatePractitionerRole(resource, lineNum)
	case "Organization":
		return validateOrganization(resource, lineNum)
	case "Location":
		return validateLocation(resource, lineNum)
	case "Endpoint":
		return validateEndpoint(resource, lineNum)
	case "OrganizationAffiliation":
		return validateOrganizationAffiliation(resource, lineNum)
	default:
		// For other resource types, check basic structure
		return validateBasicStructure(resource, lineNum)
	}
}

// validateBasicStructure checks basic FHIR structure requirements
func validateBasicStructure(resource map[string]interface{}, lineNum int) bool {
	// All FHIR resources must have resourceType
	if resource["resourceType"] == nil {
		return false
	}
	
	// If meta exists, validate it's an object
	if meta, ok := resource["meta"]; ok {
		if _, isMap := meta.(map[string]interface{}); !isMap {
			return false
		}
	}
	
	// If identifier exists, validate it's an array
	if identifier, ok := resource["identifier"]; ok {
		if _, isArray := identifier.([]interface{}); !isArray {
			return false
		}
	}
	
	return true
}

// validatePatient validates Patient resources per FHIR R4
func validatePatient(resource map[string]interface{}, lineNum int) bool {
	if !validateBasicStructure(resource, lineNum) {
		return false
	}
	
	// At least one of identifier, name, telecom, or address must be present
	hasIdentity := resource["identifier"] != nil || 
		resource["name"] != nil || 
		resource["telecom"] != nil || 
		resource["address"] != nil
	
	return hasIdentity
}

// validatePractitioner validates Practitioner resources per FHIR R4
func validatePractitioner(resource map[string]interface{}, lineNum int) bool {
	if !validateBasicStructure(resource, lineNum) {
		return false
	}
	
	// At least one of identifier, name must be present
	hasIdentity := resource["identifier"] != nil || resource["name"] != nil
	
	return hasIdentity
}

// validatePractitionerRole validates PractitionerRole resources per FHIR R4
func validatePractitionerRole(resource map[string]interface{}, lineNum int) bool {
	if !validateBasicStructure(resource, lineNum) {
		return false
	}
	
	// PractitionerRole has no required fields beyond resourceType
	return true
}

// validateOrganization validates Organization resources per FHIR R4
func validateOrganization(resource map[string]interface{}, lineNum int) bool {
	if !validateBasicStructure(resource, lineNum) {
		return false
	}
	
	// At least one of identifier, name must be present
	hasIdentity := resource["identifier"] != nil || resource["name"] != nil
	
	return hasIdentity
}

// validateLocation validates Location resources per FHIR R4
func validateLocation(resource map[string]interface{}, lineNum int) bool {
	if !validateBasicStructure(resource, lineNum) {
		return false
	}
	
	// At least one of name, identifier, type must be present
	hasIdentity := resource["name"] != nil || 
		resource["identifier"] != nil || 
		resource["type"] != nil
	
	return hasIdentity
}

// validateEndpoint validates Endpoint resources per FHIR R4
func validateEndpoint(resource map[string]interface{}, lineNum int) bool {
	if !validateBasicStructure(resource, lineNum) {
		return false
	}
	
	// Endpoints require: status, connectionType, and address
	if resource["status"] == nil {
		return false
	}
	
	if resource["connectionType"] == nil {
		return false
	}
	
	if resource["address"] == nil {
		return false
	}
	
	// Validate status is a valid code
	if status, ok := resource["status"].(string); ok {
		validStatuses := map[string]bool{
			"active": true, "suspended": true, "error": true, 
			"off": true, "entered-in-error": true, "test": true,
		}
		if !validStatuses[status] {
			return false
		}
	}
	
	return true
}

// validateOrganizationAffiliation validates OrganizationAffiliation per FHIR R4
func validateOrganizationAffiliation(resource map[string]interface{}, lineNum int) bool {
	if !validateBasicStructure(resource, lineNum) {
		return false
	}
	
	// OrganizationAffiliation has no required fields beyond resourceType
	return true
}

func printResults(stats *ValidationStats, inputFile string) {
	duration := stats.EndTime.Sub(stats.StartTime)
	
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Println("FHIR v4 Validation Results")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Input File:        %s\n", inputFile)
	fmt.Printf("Processing Time:   %s\n", duration)
	fmt.Println(strings.Repeat("-", 70))
	
	fmt.Printf("Total Records:     %d\n", stats.TotalRecords)
	fmt.Printf("Valid Records:     %d\n", stats.ValidRecords)
	fmt.Printf("Invalid Records:   %d\n", stats.InvalidRecords)
	fmt.Printf("Parse Errors:      %d\n", stats.ParseErrors)
	fmt.Println(strings.Repeat("-", 70))
	
	if stats.TotalRecords > 0 {
		validPercent := float64(stats.ValidRecords) / float64(stats.TotalRecords) * 100
		invalidPercent := float64(stats.InvalidRecords) / float64(stats.TotalRecords) * 100
		
		fmt.Printf("Validation Rate:   %.2f%% valid, %.2f%% invalid\n", 
			validPercent, invalidPercent)
		
		if stats.TotalRecords > 0 {
			recordsPerSec := float64(stats.TotalRecords) / duration.Seconds()
			fmt.Printf("Throughput:        %.0f records/second\n", recordsPerSec)
		}
	}
	
	// Print resource type breakdown
	if len(stats.ResourceCounts) > 0 {
		fmt.Println(strings.Repeat("-", 70))
		fmt.Println("Resource Type Breakdown:")
		for resourceType, count := range stats.ResourceCounts {
			percent := float64(count) / float64(stats.TotalRecords) * 100
			fmt.Printf("  %-25s %8d  (%.1f%%)\n", resourceType, count, percent)
		}
	}
	
	fmt.Println(strings.Repeat("=", 70))
}
