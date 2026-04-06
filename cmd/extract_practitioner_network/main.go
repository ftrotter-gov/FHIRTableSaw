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
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/DSACMS/FHIRTableSaw/fasttools"
)

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

	stats, err := fasttools.ExtractPractitionerNetwork(fasttools.PractitionerNetworkOptions{
		NPI:       npi,
		InputDir:  inputDir,
		OutputDir: outputDir,
		Verbose:   true,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "\nExtraction complete for NPI %s (Practitioner ID: %s)\n", npi, stats.PractitionerID)
	fmt.Fprintf(os.Stderr, "  Practitioner: %d\n", stats.PractitionersWritten)
	fmt.Fprintf(os.Stderr, "  PractitionerRole: %d\n", stats.PractitionerRolesWritten)
	fmt.Fprintf(os.Stderr, "  Organization: %d\n", stats.OrganizationsWritten)
	fmt.Fprintf(os.Stderr, "  Location: %d\n", stats.LocationsWritten)
	fmt.Fprintf(os.Stderr, "  HealthcareService: %d\n", stats.HealthcareServicesWritten)
	fmt.Fprintf(os.Stderr, "  Endpoint: %d\n", stats.EndpointsWritten)
	fmt.Fprintf(os.Stderr, "  OrganizationAffiliation: %d\n", stats.OrganizationAffiliationsWritten)
	if stats.OtherResourcesWritten > 0 {
		fmt.Fprintf(os.Stderr, "  Other resources: %d\n", stats.OtherResourcesWritten)
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
