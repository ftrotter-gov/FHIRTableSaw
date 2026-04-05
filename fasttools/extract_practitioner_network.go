// extract_practitioner_network extracts a complete network of FHIR resources related to a Practitioner by NPI.
//
// This tool performs a multi-pass extraction:
// 1. Finds the Practitioner resource with the specified NPI from Practitioner*.ndjson files
// 2. Finds all PractitionerRole resources that reference this Practitioner
// 3. Extracts all resources referenced by PractitionerRole (Organization, Location, HealthcareService, Endpoint)
// 4. Finds any other resources that directly reference the Practitioner
//
// Usage:
//
//	extract_practitioner_network <npi> <input_dir> <output_dir>
//
// The tool scans ONLY uncompressed NDJSON files (ending in .ndjson) in the input directory 
// and outputs separate NDJSON files for each resource type, named as: {npi}_{ResourceType}.ndjson
//
// Example:
//
//	extract_practitioner_network 1234567890 /path/to/ndjson /path/to/output
//
// This will create files like:
//   - 1234567890_Practitioner.ndjson
//   - 1234567890_PractitionerRole.ndjson
//   - 1234567890_Organization.ndjson
//   - 1234567890_Location.ndjson
//   - etc.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	usNPISystem          = "http://hl7.org/fhir/sid/us-npi"
	scannerBufferSize    = 256 * 1024       // 256KB initial buffer
	scannerMaxBufferSize = 50 * 1024 * 1024 // 50MB max per line
	progressInterval     = 100000            // Report progress every 100k lines
)

type ResourceSet struct {
	Practitioner       []map[string]any
	PractitionerRole   []map[string]any
	Organization       []map[string]any
	Location           []map[string]any
	HealthcareService  []map[string]any
	Endpoint           []map[string]any
	OrganizationAff    []map[string]any
	Other              map[string][]map[string]any // for any other resource types
}

func main() {
	var helpFlag bool
	flag.BoolVar(&helpFlag, "h", false, "Show help message")
	flag.BoolVar(&helpFlag, "help", false, "Show help message")
	flag.Parse()

	if helpFlag {
		usage(os.Stdout)
		os.Exit(0)
	}

	args := flag.Args()
	if len(args) != 3 {
		usage(os.Stderr)
		os.Exit(2)
	}

	npi := strings.TrimSpace(args[0])
	inputDir := args[1]
	outputDir := args[2]

	if npi == "" {
		fmt.Fprintln(os.Stderr, "error: npi must not be empty")
		usage(os.Stderr)
		os.Exit(2)
	}

	if err := run(npi, inputDir, outputDir); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprintf(w, "Usage: %s [flags] <npi> <input_dir> <output_dir>\n", os.Args[0])
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Extracts a complete network of FHIR resources related to a Practitioner by NPI.")
	fmt.Fprintln(w, "Processes ONLY uncompressed .ndjson files.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "The tool performs multi-pass extraction:")
	fmt.Fprintln(w, "  1. Finds Practitioner with specified NPI from Practitioner*.ndjson files")
	fmt.Fprintln(w, "  2. Finds all PractitionerRole resources referencing this Practitioner")
	fmt.Fprintln(w, "  3. Extracts Organizations, Locations, HealthcareServices, Endpoints from PractitionerRoles")
	fmt.Fprintln(w, "  4. Finds other resources directly referencing the Practitioner")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Flags:")
	fmt.Fprintln(w, "  -h, --help      Show this help message")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Arguments:")
	fmt.Fprintln(w, "  <npi>           The NPI number to search for")
	fmt.Fprintln(w, "  <input_dir>     Directory containing FHIR NDJSON files")
	fmt.Fprintln(w, "  <output_dir>    Directory where output files will be written")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Output files are named: {npi}_{ResourceType}.ndjson")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Example:")
	fmt.Fprintf(w, "  %s 1234567890 /data/ndjson /data/output\n", os.Args[0])
}

func run(npi, inputDir, outputDir string) error {
	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	resources := &ResourceSet{
		Other: make(map[string][]map[string]any),
	}

	// Step 1: Find the Practitioner by NPI
	fmt.Fprintf(os.Stderr, "Step 1: Searching for Practitioner with NPI %s...\n", npi)
	practitionerID, err := findPractitionerByNPI(npi, inputDir, resources)
	if err != nil {
		return err
	}
	if practitionerID == "" {
		return fmt.Errorf("no Practitioner found with NPI %s", npi)
	}
	fmt.Fprintf(os.Stderr, "  Found Practitioner: %s\n", practitionerID)

	// Step 2: Find all PractitionerRoles referencing this Practitioner
	fmt.Fprintf(os.Stderr, "Step 2: Looking for Practitioner ID %s in PractitionerRole...\n", practitionerID)
	refIDs, err := findPractitionerRoles(practitionerID, inputDir, resources)
	if err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "  Found %d PractitionerRole(s)\n", len(resources.PractitionerRole))

	// Step 3: Find related resources from PractitionerRoles
	fmt.Fprintf(os.Stderr, "Step 3: Finding related resources from PractitionerRoles...\n")
	if err := findRelatedResources(refIDs, inputDir, resources); err != nil {
		return err
	}

	// Step 4: Find Endpoints referenced by Organizations/Locations/etc.
	fmt.Fprintf(os.Stderr, "Step 4: Finding Endpoints referenced by other resources...\n")
	if err := findSecondLevelReferences(inputDir, resources); err != nil {
		return err
	}

	// Write output files
	fmt.Fprintf(os.Stderr, "Step 5: Writing output files...\n")
	if err := writeOutputFiles(npi, outputDir, resources); err != nil {
		return err
	}

	return nil
}

// findPractitionerByNPI scans all Practitioner*.ndjson files to find the Practitioner with the given NPI
func findPractitionerByNPI(npi, inputDir string, resources *ResourceSet) (string, error) {
	files, err := findNDJSONFiles(inputDir, "Practitioner")
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		return "", fmt.Errorf("no Practitioner*.ndjson files found in %s", inputDir)
	}

	var practitionerID string
	for _, file := range files {
		id, found, err := scanFileForNPI(file, npi, resources)
		if err != nil {
			return "", err
		}
		if found {
			practitionerID = id
			break
		}
	}

	return practitionerID, nil
}

// scanFileForNPI scans a file looking for a Practitioner with the given NPI
func scanFileForNPI(filePath, npi string, resources *ResourceSet) (string, bool, error) {
	scanner, closer, err := openNDJSONFile(filePath)
	if err != nil {
		return "", false, err
	}
	defer closer()

	lineNum := uint64(0)
	for scanner.Scan() {
		lineNum++
		if lineNum%progressInterval == 0 {
			fmt.Fprintf(os.Stderr, "  Scanned %d lines in %s\n", lineNum, filepath.Base(filePath))
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			continue // Skip invalid JSON
		}

		if containsNPI(obj, npi) {
			resources.Practitioner = append(resources.Practitioner, obj)
			id, _ := obj["id"].(string)
			return id, true, nil
		}
	}

	return "", false, scanner.Err()
}

// findPractitionerRoles finds all PractitionerRole resources that reference the given Practitioner ID
func findPractitionerRoles(practitionerID, inputDir string, resources *ResourceSet) (map[string][]string, error) {
	files, err := findNDJSONFiles(inputDir, "PractitionerRole")
	if err != nil {
		return nil, err
	}

	// Track all referenced IDs from PractitionerRoles (dynamic - any resource type)
	refIDs := make(map[string][]string)

	for _, file := range files {
		if err := scanPractitionerRoles(file, practitionerID, resources, refIDs); err != nil {
			return nil, err
		}
	}

	return refIDs, nil
}

func scanPractitionerRoles(filePath, practitionerID string, resources *ResourceSet, refIDs map[string][]string) error {
	scanner, closer, err := openNDJSONFile(filePath)
	if err != nil {
		return err
	}
	defer closer()

	practRef := "Practitioner/" + practitionerID
	lineNum := uint64(0)

	for scanner.Scan() {
		lineNum++
		if lineNum%progressInterval == 0 {
			fmt.Fprintf(os.Stderr, "  Scanned %d lines in %s\n", lineNum, filepath.Base(filePath))
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			continue
		}

		// Check if this PractitionerRole references our Practitioner
		if practitioner, ok := obj["practitioner"].(map[string]any); ok {
			if ref, ok := practitioner["reference"].(string); ok && ref == practRef {
				resources.PractitionerRole = append(resources.PractitionerRole, obj)

				// Collect all reference fields dynamically
				collectAllReferences(obj, refIDs)
			}
		}
	}

	return scanner.Err()
}

// collectAllReferences extracts all FHIR references from an object
func collectAllReferences(obj map[string]any, refIDs map[string][]string) {
	for _, value := range obj {
		extractReferencesFromValue(value, refIDs)
	}
}

// extractReferencesFromValue recursively extracts references from any value
func extractReferencesFromValue(value any, refIDs map[string][]string) {
	switch v := value.(type) {
	case map[string]any:
		// Check if this is a reference object
		if refStr, ok := v["reference"].(string); ok {
			// Parse ResourceType/ID format
			parts := strings.SplitN(refStr, "/", 2)
			if len(parts) == 2 {
				resourceType := parts[0]
				resourceID := parts[1]
				// Skip Practitioner references - we already have the Practitioner
				if resourceType != "" && resourceID != "" && resourceType != "Practitioner" {
					refIDs[resourceType] = append(refIDs[resourceType], resourceID)
				}
			}
		}
		// Recursively search nested objects
		for _, nested := range v {
			extractReferencesFromValue(nested, refIDs)
		}
	case []any:
		// Recursively search arrays
		for _, item := range v {
			extractReferencesFromValue(item, refIDs)
		}
	}
}

// findRelatedResources finds all resources referenced by PractitionerRoles
func findRelatedResources(refIDs map[string][]string, inputDir string, resources *ResourceSet) error {
	for resourceType, ids := range refIDs {
		if len(ids) == 0 {
			continue
		}

		// Deduplicate IDs
		idSet := makeIDSet(ids)
		uniqueCount := len(idSet)
		
		fmt.Fprintf(os.Stderr, "  Looking for %d different %s resource(s) in %s.ndjson...\n", 
			uniqueCount, resourceType, resourceType)

		files, err := findNDJSONFiles(inputDir, resourceType)
		if err != nil {
			return err
		}

		for _, file := range files {
			if err := scanForIDs(file, resourceType, idSet, resources); err != nil {
				return err
			}
		}
	}

	return nil
}

func scanForIDs(filePath, resourceType string, idSet map[string]bool, resources *ResourceSet) error {
	scanner, closer, err := openNDJSONFile(filePath)
	if err != nil {
		return err
	}
	defer closer()

	lineNum := uint64(0)
	found := 0
	
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			continue
		}

		id, _ := obj["id"].(string)
		if id != "" && idSet[id] {
			found++
			storeResource(resourceType, obj, resources)
		}
	}
	
	if found > 0 {
		fmt.Fprintf(os.Stderr, "    Found %d %s resource(s) in %s\n", found, resourceType, filepath.Base(filePath))
	}

	return scanner.Err()
}

// storeResource stores a resource in the appropriate collection
func storeResource(resourceType string, obj map[string]any, resources *ResourceSet) {
	switch resourceType {
	case "Organization":
		resources.Organization = append(resources.Organization, obj)
	case "Location":
		resources.Location = append(resources.Location, obj)
	case "HealthcareService":
		resources.HealthcareService = append(resources.HealthcareService, obj)
	case "Endpoint":
		resources.Endpoint = append(resources.Endpoint, obj)
	default:
		if resources.Other[resourceType] == nil {
			resources.Other[resourceType] = []map[string]any{}
		}
		resources.Other[resourceType] = append(resources.Other[resourceType], obj)
	}
}

// findSecondLevelReferences finds Endpoints referenced by Organizations, Locations, etc.
func findSecondLevelReferences(inputDir string, resources *ResourceSet) error {
	// Collect all endpoint references from already-found resources
	endpointIDs := make(map[string][]string)
	
	// Check Organizations
	for _, org := range resources.Organization {
		collectAllReferences(org, endpointIDs)
	}
	
	// Check Locations
	for _, loc := range resources.Location {
		collectAllReferences(loc, endpointIDs)
	}
	
	// Check HealthcareServices
	for _, hcs := range resources.HealthcareService {
		collectAllReferences(hcs, endpointIDs)
	}
	
	// If we found endpoint references, search for them
	if ids, ok := endpointIDs["Endpoint"]; ok && len(ids) > 0 {
		idSet := makeIDSet(ids)
		uniqueCount := len(idSet)
		
		fmt.Fprintf(os.Stderr, "  Looking for %d different Endpoint resource(s) in Endpoint.ndjson...\n", uniqueCount)
		
		files, err := findNDJSONFiles(inputDir, "Endpoint")
		if err != nil {
			return err
		}
		
		for _, file := range files {
			if err := scanForIDs(file, "Endpoint", idSet, resources); err != nil {
				return err
			}
		}
	}
	
	return nil
}

// writeOutputFiles writes all collected resources to separate NDJSON files
func writeOutputFiles(npi, outputDir string, resources *ResourceSet) error {
	if err := writeResourceFile(npi, outputDir, "Practitioner", resources.Practitioner); err != nil {
		return err
	}
	if err := writeResourceFile(npi, outputDir, "PractitionerRole", resources.PractitionerRole); err != nil {
		return err
	}
	if err := writeResourceFile(npi, outputDir, "Organization", resources.Organization); err != nil {
		return err
	}
	if err := writeResourceFile(npi, outputDir, "Location", resources.Location); err != nil {
		return err
	}
	if err := writeResourceFile(npi, outputDir, "HealthcareService", resources.HealthcareService); err != nil {
		return err
	}
	if err := writeResourceFile(npi, outputDir, "Endpoint", resources.Endpoint); err != nil {
		return err
	}

	// Write other resource types
	for resourceType, objs := range resources.Other {
		if err := writeResourceFile(npi, outputDir, resourceType, objs); err != nil {
			return err
		}
	}

	return nil
}

func writeResourceFile(npi, outputDir, resourceType string, resources []map[string]any) error {
	if len(resources) == 0 {
		return nil // Don't create empty files
	}

	filename := fmt.Sprintf("%s_%s.ndjson", npi, resourceType)
	filePath := filepath.Join(outputDir, filename)

	f, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create %s: %w", filename, err)
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	for _, obj := range resources {
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("failed to encode resource to %s: %w", filename, err)
		}
	}

	fmt.Fprintf(os.Stderr, "  Wrote %d resources to %s\n", len(resources), filename)
	return nil
}

// Helper functions

// findNDJSONFiles finds all files matching ResourceType*.ndjson (ONLY .ndjson, no compressed files)
func findNDJSONFiles(inputDir, resourceType string) ([]string, error) {
	pattern := filepath.Join(inputDir, resourceType+"*.ndjson")
	allMatches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to glob %s files: %w", resourceType, err)
	}

	// Filter to ONLY files ending in exactly .ndjson and starting with exact resource type
	var files []string
	for _, match := range allMatches {
		baseName := filepath.Base(match)
		lower := strings.ToLower(baseName)
		lowerResourceType := strings.ToLower(resourceType)
		
		// Must end with .ndjson
		if !strings.HasSuffix(lower, ".ndjson") {
			continue
		}
		
		// Must NOT have additional extensions after .ndjson (like .ndjson.gz, .ndjson.zst)
		// Check last 20 chars or entire string if shorter
		checkLen := 20
		if len(lower) < checkLen {
			checkLen = len(lower)
		}
		endPart := lower[len(lower)-checkLen:]
		if strings.Contains(endPart, ".ndjson.") {
			continue
		}
		
		// Must start with ResourceType followed by non-letter (to avoid Practitioner matching PractitionerRole)
		if !strings.HasPrefix(lower, lowerResourceType) {
			continue
		}
		
		// Check what comes after the resource type name
		afterType := lower[len(lowerResourceType):]
		if len(afterType) > 0 {
			firstChar := rune(afterType[0])
			// Must be non-letter (like _, ., or number) - not another letter
			if (firstChar >= 'a' && firstChar <= 'z') {
				continue
			}
		}
		
		files = append(files, match)
	}

	return files, nil
}

func openNDJSONFile(filePath string) (*bufio.Scanner, func(), error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, scannerBufferSize), scannerMaxBufferSize)

	closer := func() {
		f.Close()
	}

	return scanner, closer, nil
}

func containsNPI(obj map[string]any, targetNPI string) bool {
	idents, ok := obj["identifier"]
	if !ok {
		return false
	}
	arr, ok := idents.([]any)
	if !ok {
		return false
	}
	targetNPILower := strings.ToLower(targetNPI)
	for _, item := range arr {
		im, ok := item.(map[string]any)
		if !ok {
			continue
		}
		sys, _ := im["system"].(string)
		val, _ := im["value"].(string)
		if sys == usNPISystem && strings.ToLower(val) == targetNPILower {
			return true
		}
	}
	return false
}

func containsReference(obj map[string]any, targetRef string) bool {
	// Recursively search for any reference to the target
	return searchForReference(obj, targetRef)
}

func searchForReference(v any, targetRef string) bool {
	switch val := v.(type) {
	case map[string]any:
		// Check if this is a reference object
		if ref, ok := val["reference"].(string); ok && ref == targetRef {
			return true
		}
		// Recursively search nested maps
		for _, nested := range val {
			if searchForReference(nested, targetRef) {
				return true
			}
		}
	case []any:
		// Recursively search arrays
		for _, item := range val {
			if searchForReference(item, targetRef) {
				return true
			}
		}
	}
	return false
}

func makeIDSet(ids []string) map[string]bool {
	set := make(map[string]bool)
	for _, id := range ids {
		set[id] = true
	}
	return set
}
