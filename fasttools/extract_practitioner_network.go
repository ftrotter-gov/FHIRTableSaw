// Package fasttools contains small utilities for working with FHIR NDJSON data.
package fasttools

import (
	"bufio"
	"encoding/json"
	"fmt"
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

// PractitionerNetworkOptions contains options for extracting a practitioner network
type PractitionerNetworkOptions struct {
	NPI       string
	InputDir  string
	OutputDir string
	Verbose   bool
}

// PractitionerNetworkStats contains statistics from the extraction
type PractitionerNetworkStats struct {
	PractitionerID               string
	PractitionersWritten         int
	PractitionerRolesWritten     int
	OrganizationsWritten         int
	LocationsWritten             int
	HealthcareServicesWritten    int
	EndpointsWritten             int
	OrganizationAffiliationsWritten int
	OtherResourcesWritten        int
}

// ResourceSet holds all extracted resources
type ResourceSet struct {
	Practitioner      []map[string]any
	PractitionerRole  []map[string]any
	Organization      []map[string]any
	Location          []map[string]any
	HealthcareService []map[string]any
	Endpoint          []map[string]any
	OrganizationAff   []map[string]any
	Other             map[string][]map[string]any // for any other resource types
}

// ExtractPractitionerNetwork extracts a complete network of FHIR resources related to a Practitioner by NPI.
//
// This performs a multi-pass extraction:
// 1. Finds the Practitioner resource with the specified NPI from Practitioner*.ndjson files
// 2. Finds all PractitionerRole resources that reference this Practitioner
// 3. Extracts all resources referenced by PractitionerRole (Organization, Location, HealthcareService, Endpoint)
// 4. Finds any other resources that directly reference the Practitioner
func ExtractPractitionerNetwork(opts PractitionerNetworkOptions) (*PractitionerNetworkStats, error) {
	// Ensure output directory exists
	if err := os.MkdirAll(opts.OutputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	resources := &ResourceSet{
		Other: make(map[string][]map[string]any),
	}

	// Step 1: Find the Practitioner by NPI
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "Step 1: Searching for Practitioner with NPI %s...\n", opts.NPI)
	}
	practitionerID, err := findPractitionerByNPI(opts.NPI, opts.InputDir, resources, opts.Verbose)
	if err != nil {
		return nil, err
	}
	if practitionerID == "" {
		return nil, fmt.Errorf("no Practitioner found with NPI %s", opts.NPI)
	}
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "  Found Practitioner: %s\n", practitionerID)
	}

	// Step 2: Find all PractitionerRoles referencing this Practitioner
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "Step 2: Looking for Practitioner ID %s in PractitionerRole...\n", practitionerID)
	}
	refIDs, err := findPractitionerRoles(practitionerID, opts.InputDir, resources, opts.Verbose)
	if err != nil {
		return nil, err
	}
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "  Found %d PractitionerRole(s)\n", len(resources.PractitionerRole))
	}

	// Step 3: Find related resources from PractitionerRoles
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "Step 3: Finding related resources from PractitionerRoles...\n")
	}
	if err := findRelatedResources(refIDs, opts.InputDir, resources, opts.Verbose); err != nil {
		return nil, err
	}

	// Step 4: Find Endpoints referenced by Organizations/Locations/etc.
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "Step 4: Finding Endpoints referenced by other resources...\n")
	}
	if err := findSecondLevelReferences(opts.InputDir, resources, opts.Verbose); err != nil {
		return nil, err
	}

	// Write output files
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "Step 5: Writing output files...\n")
	}
	stats := &PractitionerNetworkStats{
		PractitionerID: practitionerID,
	}
	if err := writePractitionerOutputFiles(opts.NPI, opts.OutputDir, resources, stats, opts.Verbose); err != nil {
		return nil, err
	}

	return stats, nil
}

// findPractitionerByNPI scans all Practitioner*.ndjson files to find the Practitioner with the given NPI
func findPractitionerByNPI(npi, inputDir string, resources *ResourceSet, verbose bool) (string, error) {
	files, err := findNDJSONFiles(inputDir, "Practitioner")
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		return "", fmt.Errorf("no Practitioner*.ndjson files found in %s", inputDir)
	}

	var practitionerID string
	for _, file := range files {
		id, found, err := scanFileForNPI(file, npi, resources, verbose)
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
func scanFileForNPI(filePath, npi string, resources *ResourceSet, verbose bool) (string, bool, error) {
	scanner, closer, err := openNDJSONFile(filePath)
	if err != nil {
		return "", false, err
	}
	defer closer()

	lineNum := uint64(0)
	for scanner.Scan() {
		lineNum++
		if verbose && lineNum%progressInterval == 0 {
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
func findPractitionerRoles(practitionerID, inputDir string, resources *ResourceSet, verbose bool) (map[string][]string, error) {
	files, err := findNDJSONFiles(inputDir, "PractitionerRole")
	if err != nil {
		return nil, err
	}

	// Track all referenced IDs from PractitionerRoles (dynamic - any resource type)
	refIDs := make(map[string][]string)

	for _, file := range files {
		if err := scanPractitionerRoles(file, practitionerID, resources, refIDs, verbose); err != nil {
			return nil, err
		}
	}

	return refIDs, nil
}

func scanPractitionerRoles(filePath, practitionerID string, resources *ResourceSet, refIDs map[string][]string, verbose bool) error {
	scanner, closer, err := openNDJSONFile(filePath)
	if err != nil {
		return err
	}
	defer closer()

	practRef := "Practitioner/" + practitionerID
	lineNum := uint64(0)

	for scanner.Scan() {
		lineNum++
		if verbose && lineNum%progressInterval == 0 {
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
				if !contains(refIDs[resourceType], resourceID) {
					refIDs[resourceType] = append(refIDs[resourceType], resourceID)
				}
			}
		}
		// Recursively check nested objects
		for _, nested := range v {
			extractReferencesFromValue(nested, refIDs)
		}
	case []any:
		// Recursively check array elements
		for _, item := range v {
			extractReferencesFromValue(item, refIDs)
		}
	}
}

// findRelatedResources finds all resources referenced by PractitionerRoles
func findRelatedResources(refIDs map[string][]string, inputDir string, resources *ResourceSet, verbose bool) error {
	for resourceType, ids := range refIDs {
		if len(ids) == 0 {
			continue
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "  Looking for %d %s resource(s)...\n", len(ids), resourceType)
		}

		files, err := findNDJSONFiles(inputDir, resourceType)
		if err != nil {
			return err
		}

		for _, file := range files {
			if err := scanForIDs(file, resourceType, ids, resources, verbose); err != nil {
				return err
			}
		}

		if verbose {
			count := getResourceCount(resources, resourceType)
			fmt.Fprintf(os.Stderr, "    Found %d %s resource(s)\n", count, resourceType)
		}
	}

	return nil
}

// scanForIDs scans a file looking for resources with specific IDs
func scanForIDs(filePath, resourceType string, targetIDs []string, resources *ResourceSet, verbose bool) error {
	scanner, closer, err := openNDJSONFile(filePath)
	if err != nil {
		return err
	}
	defer closer()

	lineNum := uint64(0)
	for scanner.Scan() {
		lineNum++
		if verbose && lineNum%progressInterval == 0 {
			fmt.Fprintf(os.Stderr, "    Scanned %d lines in %s\n", lineNum, filepath.Base(filePath))
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal(line, &obj); err != nil {
			continue
		}

		id, _ := obj["id"].(string)
		if contains(targetIDs, id) {
			addToResourceSet(resources, resourceType, obj)
		}
	}

	return scanner.Err()
}

// findSecondLevelReferences finds Endpoints and other resources referenced by already-found resources
func findSecondLevelReferences(inputDir string, resources *ResourceSet, verbose bool) error {
	secondLevelRefs := make(map[string][]string)

	// Collect references from all currently found resources
	for _, res := range resources.Organization {
		collectAllReferences(res, secondLevelRefs)
	}
	for _, res := range resources.Location {
		collectAllReferences(res, secondLevelRefs)
	}
	for _, res := range resources.HealthcareService {
		collectAllReferences(res, secondLevelRefs)
	}
	for _, res := range resources.OrganizationAff {
		collectAllReferences(res, secondLevelRefs)
	}

	// Find these second-level resources
	return findRelatedResources(secondLevelRefs, inputDir, resources, verbose)
}

// writePractitionerOutputFiles writes all extracted resources to separate NDJSON files
func writePractitionerOutputFiles(npi, outputDir string, resources *ResourceSet, stats *PractitionerNetworkStats, verbose bool) error {
	writeResource := func(resourceType string, items []map[string]any) error {
		if len(items) == 0 {
			return nil
		}

		filename := filepath.Join(outputDir, fmt.Sprintf("%s_%s.ndjson", npi, resourceType))
		f, err := os.Create(filename)
		if err != nil {
			return fmt.Errorf("failed to create %s: %w", filename, err)
		}
		defer f.Close()

		enc := json.NewEncoder(f)
		for _, item := range items {
			if err := enc.Encode(item); err != nil {
				return fmt.Errorf("failed to write to %s: %w", filename, err)
			}
		}

		if verbose {
			fmt.Fprintf(os.Stderr, "  Wrote %d resource(s) to %s\n", len(items), filepath.Base(filename))
		}
		return nil
	}

	if err := writeResource("Practitioner", resources.Practitioner); err != nil {
		return err
	}
	stats.PractitionersWritten = len(resources.Practitioner)

	if err := writeResource("PractitionerRole", resources.PractitionerRole); err != nil {
		return err
	}
	stats.PractitionerRolesWritten = len(resources.PractitionerRole)

	if err := writeResource("Organization", resources.Organization); err != nil {
		return err
	}
	stats.OrganizationsWritten = len(resources.Organization)

	if err := writeResource("Location", resources.Location); err != nil {
		return err
	}
	stats.LocationsWritten = len(resources.Location)

	if err := writeResource("HealthcareService", resources.HealthcareService); err != nil {
		return err
	}
	stats.HealthcareServicesWritten = len(resources.HealthcareService)

	if err := writeResource("Endpoint", resources.Endpoint); err != nil {
		return err
	}
	stats.EndpointsWritten = len(resources.Endpoint)

	if err := writeResource("OrganizationAffiliation", resources.OrganizationAff); err != nil {
		return err
	}
	stats.OrganizationAffiliationsWritten = len(resources.OrganizationAff)

	// Write any other resource types
	for resourceType, items := range resources.Other {
		if err := writeResource(resourceType, items); err != nil {
			return err
		}
		stats.OtherResourcesWritten += len(items)
	}

	return nil
}

// Helper functions

func addToResourceSet(resources *ResourceSet, resourceType string, obj map[string]any) {
	switch resourceType {
	case "Organization":
		resources.Organization = append(resources.Organization, obj)
	case "Location":
		resources.Location = append(resources.Location, obj)
	case "HealthcareService":
		resources.HealthcareService = append(resources.HealthcareService, obj)
	case "Endpoint":
		resources.Endpoint = append(resources.Endpoint, obj)
	case "OrganizationAffiliation":
		resources.OrganizationAff = append(resources.OrganizationAff, obj)
	default:
		resources.Other[resourceType] = append(resources.Other[resourceType], obj)
	}
}

func getResourceCount(resources *ResourceSet, resourceType string) int {
	switch resourceType {
	case "Organization":
		return len(resources.Organization)
	case "Location":
		return len(resources.Location)
	case "HealthcareService":
		return len(resources.HealthcareService)
	case "Endpoint":
		return len(resources.Endpoint)
	case "OrganizationAffiliation":
		return len(resources.OrganizationAff)
	default:
		return len(resources.Other[resourceType])
	}
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

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func findNDJSONFiles(dir, prefix string) ([]string, error) {
	pattern := filepath.Join(dir, prefix+"*")
	allFiles, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("failed to find %s files: %w", prefix, err)
	}
	
	// ONLY accept files ending in .ndjson (not .ndjson.gz or any other extension)
	var files []string
	for _, f := range allFiles {
		if strings.HasSuffix(strings.ToLower(f), ".ndjson") {
			files = append(files, f)
		}
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
		_ = f.Close()
	}

	return scanner, closer, nil
}
