// wyomingizer extracts a state-focused subset of FHIR NDJSON.
//
// It uses Location.address.state as the seed and then performs a bounded traversal
// over a handful of resource types to keep the resulting output small.
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/DSACMS/FHIRTableSaw/fasttools"
)

func main() {
	inputDir := flag.String("input_dir", "", "Directory containing input NDJSON shards")
	outputDir := flag.String("output_dir", "", "Directory to write subset NDJSON files")
	states := flag.String("states", "", "Comma-separated state codes, e.g. WY,RI")
	overwrite := flag.Bool("overwrite", false, "Overwrite output files if they already exist")
	verbose := flag.Bool("verbose", false, "Print progress to stderr")
	flag.Parse()

	if strings.TrimSpace(*inputDir) == "" || strings.TrimSpace(*outputDir) == "" || strings.TrimSpace(*states) == "" {
		flag.Usage()
		os.Exit(2)
	}

	stateList := splitStates(*states)
	st, err := fasttools.Wyomingize(fasttools.WyomingizeOptions{
		InputDir:  *inputDir,
		OutputDir: *outputDir,
		States:    stateList,
		Overwrite: *overwrite,
		Verbose:   *verbose,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "states=%s wrote Location=%d Organization=%d Practitioner=%d PractitionerRole=%d OrganizationAffiliation=%d Endpoint=%d invalid_json=%d\n",
		st.StatesKey,
		st.LocationsWritten,
		st.OrganizationsWritten,
		st.PractitionersWritten,
		st.PractitionerRolesWritten,
		st.OrganizationAffiliationsWritten,
		st.EndpointsWritten,
		st.InvalidJSONLines,
	)
}

func splitStates(s string) []string {
	// Accept commas and/or whitespace.
	fields := strings.FieldsFunc(s, func(r rune) bool {
		return r == ',' || r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})
	var out []string
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}
