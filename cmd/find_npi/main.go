// find_npi is a small "fast tool" to extract matching FHIR resources from an NDJSON file.
//
// It scans a FHIR Bulk Export style NDJSON file (one JSON object per line), finds resources
// that contain a specific NPI (identifier.system == "http://hl7.org/fhir/sid/us-npi" and
// identifier.value == <NPI>), and writes the matching resources to an output file.
//
// Usage:
//
//	go run ./cmd/find_npi <npi> <input.ndjson> <output.json>
//
// Flags:
//   -h, --help      Show help message
//   --ndjson        Output as NDJSON (one resource per line) instead of JSON array
//
// Notes:
//   - The input may be plain NDJSON or gzip-compressed (*.gz) by extension, or '-' for stdin.
//   - Output can be a file path or '-' for stdout.
//   - Output is a JSON array (pretty printed with indentation) by default, or NDJSON with --ndjson flag.
//   - Invalid JSON lines are skipped (reported to stderr) to better handle real-world data.
//   - NPI matching is case-insensitive.
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	usNPISystem          = "http://hl7.org/fhir/sid/us-npi"
	scannerBufferSize    = 256 * 1024       // 256KB initial buffer
	scannerMaxBufferSize = 50 * 1024 * 1024 // 50MB max per line
	progressInterval     = 100000            // Report progress every 100k lines
	errorSnippetLength   = 100               // Show first 100 chars of invalid JSON
)

func main() {
	var (
		helpFlag   bool
		ndjsonFlag bool
	)

	flag.BoolVar(&helpFlag, "h", false, "Show help message")
	flag.BoolVar(&helpFlag, "help", false, "Show help message")
	flag.BoolVar(&ndjsonFlag, "ndjson", false, "Output as NDJSON instead of JSON array")
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
	inputPath := args[1]
	outputPath := args[2]

	if npi == "" {
		fmt.Fprintln(os.Stderr, "error: npi must not be empty")
		usage(os.Stderr)
		os.Exit(2)
	}

	if err := run(npi, inputPath, outputPath, ndjsonFlag); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprintf(w, "Usage: %s [flags] <npi> <input.ndjson|input.ndjson.gz|-> <output.json|->\n", os.Args[0])
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Scans a FHIR NDJSON file and outputs resources containing the specified NPI.")
	fmt.Fprintln(w, "The NPI is detected in identifier[] where system == http://hl7.org/fhir/sid/us-npi.")
	fmt.Fprintln(w, "NPI matching is case-insensitive.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Flags:")
	fmt.Fprintln(w, "  -h, --help      Show this help message")
	fmt.Fprintln(w, "  --ndjson        Output as NDJSON (one resource per line) instead of JSON array")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Arguments:")
	fmt.Fprintln(w, "  <npi>           The NPI number to search for")
	fmt.Fprintln(w, "  <input>         Input file path (use '-' for stdin)")
	fmt.Fprintln(w, "  <output>        Output file path (use '-' for stdout)")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Examples:")
	fmt.Fprintf(w, "  %s 1234567890 practitioners.ndjson matches.json\n", os.Args[0])
	fmt.Fprintf(w, "  %s 1234567890 practitioners.ndjson.gz matches.json\n", os.Args[0])
	fmt.Fprintf(w, "  %s --ndjson 1234567890 - output.ndjson\n", os.Args[0])
	fmt.Fprintf(w, "  %s 1234567890 input.ndjson - | jq .\n", os.Args[0])
}

func run(npi, inputPath, outputPath string, ndjsonOutput bool) error {
	var r io.Reader
	var closer io.Closer

	if inputPath == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(inputPath)
		if err != nil {
			return err
		}
		r = f
		closer = f

		if strings.HasSuffix(strings.ToLower(inputPath), ".gz") {
			gz, err := gzip.NewReader(f)
			if err != nil {
				_ = f.Close()
				return err
			}
			r = gz
			closer = multiCloser{first: gz, second: f}
		}
	}
	if closer != nil {
		defer func() { _ = closer.Close() }()
	}

	var outF io.WriteCloser
	var outCloser io.Closer

	if outputPath == "-" {
		outF = os.Stdout
	} else {
		f, err := os.Create(outputPath)
		if err != nil {
			return err
		}
		outF = f
		outCloser = f
	}
	if outCloser != nil {
		defer func() { _ = outCloser.Close() }()
	}

	enc := json.NewEncoder(outF)
	if !ndjsonOutput {
		enc.SetIndent("", "  ")
	}

	// For JSON array output, write the opening bracket
	if !ndjsonOutput {
		if _, err := outF.Write([]byte("[\n")); err != nil {
			return err
		}
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, scannerBufferSize), scannerMaxBufferSize)

	var (
		lineNum      uint64
		matchCount   uint64
		invalidCount uint64
	)

	for scanner.Scan() {
		lineNum++

		// Progress reporting for large files
		if lineNum%progressInterval == 0 {
			fmt.Fprintf(os.Stderr, "progress: processed %d lines, found %d matches\n", lineNum, matchCount)
		}

		line := bytesTrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		// Quick pre-check: does the line even contain the NPI string?
		// This can save unmarshaling for non-matching records
		if !containsNPIString(line, npi) {
			continue
		}

		// Decode only what we need for matching, but keep the full object for output.
		var obj any
		if err := json.Unmarshal(line, &obj); err != nil {
			invalidCount++
			snippet := string(line)
			if len(snippet) > errorSnippetLength {
				snippet = snippet[:errorSnippetLength] + "..."
			}
			fmt.Fprintf(os.Stderr, "warn: invalid JSON on line %d: %v\n      snippet: %s\n", lineNum, err, snippet)
			continue
		}

		if !containsNPI(obj, npi) {
			continue
		}

		// For JSON array output, add comma before subsequent elements
		if !ndjsonOutput && matchCount > 0 {
			if _, err := outF.Write([]byte(",\n")); err != nil {
				return err
			}
		}

		// Encode the matching resource
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("failed to write output JSON (line %d): %w", lineNum, err)
		}
		matchCount++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan error: %w", err)
	}

	// For JSON array output, close the array
	if !ndjsonOutput {
		if _, err := outF.Write([]byte("]\n")); err != nil {
			return err
		}
	}

	if invalidCount > 0 {
		fmt.Fprintf(os.Stderr, "note: skipped %d invalid JSON line(s)\n", invalidCount)
	}
	outDesc := outputPath
	if outputPath == "-" {
		outDesc = "stdout"
	}
	fmt.Fprintf(os.Stderr, "wrote %d matching resource(s) to %s (processed %d lines)\n", matchCount, outDesc, lineNum)
	return nil
}

// containsNPIString does a fast string search to see if the raw JSON line contains the NPI.
// This is a pre-filter optimization to avoid unmarshaling JSON for non-matching lines.
func containsNPIString(line []byte, targetNPI string) bool {
	return strings.Contains(string(line), targetNPI)
}

// containsNPI checks for the NPI in identifier[]. This is written against generic JSON
// (map[string]any) to avoid needing generated FHIR structs.
// NPI matching is case-insensitive.
func containsNPI(obj any, targetNPI string) bool {
	m, ok := obj.(map[string]any)
	if !ok {
		return false
	}
	idents, ok := m["identifier"]
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

type multiCloser struct {
	first  io.Closer
	second io.Closer
}

func (m multiCloser) Close() error {
	e1 := m.first.Close()
	e2 := m.second.Close()
	return errors.Join(e1, e2)
}

// bytesTrimSpace is a tiny helper to avoid converting []byte->string for TrimSpace.
func bytesTrimSpace(b []byte) []byte {
	start := 0
	end := len(b)
	for start < end {
		switch b[start] {
		case ' ', '\t', '\n', '\r':
			start++
		default:
			goto right
		}
	}
right:
	for end > start {
		switch b[end-1] {
		case ' ', '\t', '\n', '\r':
			end--
		default:
			return b[start:end]
		}
	}
	return b[start:end]
}
