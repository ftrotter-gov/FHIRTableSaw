// find_npi is a small “fast tool” to extract matching FHIR resources from an NDJSON file.
//
// It scans a FHIR Bulk Export style NDJSON file (one JSON object per line), finds resources
// that contain a specific NPI (identifier.system == "http://hl7.org/fhir/sid/us-npi" and
// identifier.value == <NPI>), and writes the matching resources to an output file as
// pretty-printed JSON.
//
// Usage:
//
//	go run fasttools/find_npi.go <npi> <input.ndjson> <output.json>
//
// Notes:
//   - The input may be plain NDJSON or gzip-compressed (*.gz) by extension.
//   - Output is a JSON array (pretty printed with indentation), suitable for inspection.
//   - Invalid JSON lines are skipped (reported to stderr) to better handle real-world data.
package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const usNPISystem = "http://hl7.org/fhir/sid/us-npi"

func main() {
	if len(os.Args) != 4 {
		usage(os.Stderr)
		os.Exit(2)
	}

	npi := strings.TrimSpace(os.Args[1])
	inputPath := os.Args[2]
	outputPath := os.Args[3]

	if npi == "" {
		fmt.Fprintln(os.Stderr, "error: npi must not be empty")
		usage(os.Stderr)
		os.Exit(2)
	}

	if err := run(npi, inputPath, outputPath); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func usage(w io.Writer) {
	fmt.Fprintf(w, "Usage: %s <npi> <input.ndjson|input.ndjson.gz|-> <output.json>\n", os.Args[0])
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Scans a FHIR NDJSON file and outputs pretty-printed JSON for resources containing the NPI.")
	fmt.Fprintln(w, "The NPI is detected in identifier[] where system == http://hl7.org/fhir/sid/us-npi.")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Examples:")
	fmt.Fprintf(w, "  %s 1234567890 practitioners.ndjson matches.json\n", os.Args[0])
	fmt.Fprintf(w, "  %s 1234567890 practitioners.ndjson.gz matches.json\n", os.Args[0])
}

func run(npi, inputPath, outputPath string) error {
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

	outF, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer func() { _ = outF.Close() }()

	// Stream in and stream out. We build the output JSON array incrementally.
	enc := json.NewEncoder(outF)
	enc.SetIndent("", "  ")

	// We'll write the JSON array manually so we don't have to store all matches in memory.
	if _, err := outF.WriteString("[\n"); err != nil {
		return err
	}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 256*1024), 50*1024*1024) // up to 50MB per line

	var (
		lineNum      uint64
		matchCount   uint64
		invalidCount uint64
	)

	for scanner.Scan() {
		lineNum++
		line := bytesTrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}

		// Decode only what we need for matching, but keep the full object for output.
		var obj any
		if err := json.Unmarshal(line, &obj); err != nil {
			invalidCount++
			fmt.Fprintf(os.Stderr, "warn: invalid JSON on line %d: %v\n", lineNum, err)
			continue
		}

		if !containsNPI(obj, npi) {
			continue
		}

		if matchCount > 0 {
			if _, err := outF.WriteString(",\n"); err != nil {
				return err
			}
		}

		// Encode the matching resource. json.Encoder adds a trailing newline, which is fine.
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("failed to write output JSON (line %d): %w", lineNum, err)
		}
		matchCount++
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan error: %w", err)
	}

	// Close array; keep output nicely formatted.
	if matchCount > 0 {
		// json.Encoder added a newline after the last element; keep it.
		if _, err := outF.WriteString("]\n"); err != nil {
			return err
		}
	} else {
		if _, err := outF.WriteString("]\n"); err != nil {
			return err
		}
	}

	if invalidCount > 0 {
		fmt.Fprintf(os.Stderr, "note: skipped %d invalid JSON line(s)\n", invalidCount)
	}
	fmt.Fprintf(os.Stderr, "wrote %d matching resource(s) to %s\n", matchCount, outputPath)
	return nil
}

// containsNPI checks for the NPI in identifier[]. This is written against generic JSON
// (map[string]any) to avoid needing generated FHIR structs.
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
	for _, item := range arr {
		im, ok := item.(map[string]any)
		if !ok {
			continue
		}
		sys, _ := im["system"].(string)
		val, _ := im["value"].(string)
		if sys == usNPISystem && val == targetNPI {
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
