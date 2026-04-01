// categorize_ndjson is a tiny “fast tool” for doing a first-pass inspection of FHIR NDJSON.
//
// It streams one-or-more NDJSON files (or stdin) and counts how many resources of each
// resourceType are present, then prints the counts to STDOUT.
//
// Typical usage:
//
//	go run fasttools/categorize_ndjson.go ./some_file.ndjson
//	go run fasttools/categorize_ndjson.go ./dir_with_ndjson_files
//	cat some_file.ndjson | go run fasttools/categorize_ndjson.go -
//
// Notes:
//   - Each line is expected to be a single FHIR resource JSON object (Bulk Export style).
//   - .gz files are supported (by extension) and are transparently decompressed.
//   - Invalid JSON lines are skipped (unless -strict).
package main

import (
	"bufio"
	"compress/gzip"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type headerOnly struct {
	ResourceType string `json:"resourceType"`
}

type counts struct {
	byType       map[string]uint64
	totalLines   uint64
	invalidLines uint64
	emptyLines   uint64
	md5Hex       string
	sizeBytes    uint64
	hasSize      bool
}

func newCounts() *counts {
	return &counts{byType: make(map[string]uint64)}
}

func main() {
	strict := flag.Bool("strict", false, "fail fast on the first invalid JSON line")
	noHeader := flag.Bool("no-header", false, "do not print the header line")
	precount := flag.Bool("precount", true, "pre-scan inputs (like wc -l) to enable progress reporting")
	progress := flag.Bool("progress", true, "print a progress bar to stderr while scanning")
	printMD5 := flag.Bool("md5", true, "print md5sum for each input file at the end of output")
	printSize := flag.Bool("size", true, "print file size in bytes for each input file at the end of output")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <file|dir|->...\n\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "Streams NDJSON (FHIR Bulk Export style) and counts resourceType occurrences.")
		fmt.Fprintln(os.Stderr, "If no paths are provided, stdin is read.")
		fmt.Fprintln(os.Stderr, "Paths may be files, directories (walked recursively), or '-' for stdin.")
		fmt.Fprintln(os.Stderr, "\nFlags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		args = []string{"-"}
	}

	// Expand args into an ordered list of inputs.
	inputs, err := expandInputs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}
	if len(inputs) == 0 {
		fmt.Fprintln(os.Stderr, "error: no inputs found")
		os.Exit(2)
	}

	opts := options{
		strict:    *strict,
		precount:  *precount,
		progress:  *progress,
		printMD5:  *printMD5,
		printSize: *printSize,
	}

	c := newCounts()
	for idx, in := range inputs {
		if idx > 0 {
			fmt.Fprintln(os.Stdout)
		}
		fmt.Fprintf(os.Stdout, "FILE\t%s\n", in)

		// New counts per input.
		c = newCounts()
		if err := processInput(in, c, opts); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		printCounts(os.Stdout, c, !*noHeader)
	}
}

type options struct {
	strict    bool
	precount  bool
	progress  bool
	printMD5  bool
	printSize bool
}

// expandInputs turns the raw CLI args into concrete stream inputs.
// Directories are walked recursively, returning any *.ndjson, *.jsonl, *.ndjson.gz, *.jsonl.gz.
// "-" means stdin.
func expandInputs(args []string) ([]string, error) {
	var out []string
	for _, a := range args {
		if a == "-" {
			out = append(out, "-")
			continue
		}

		fi, err := os.Stat(a)
		if err != nil {
			return nil, err
		}
		if !fi.IsDir() {
			out = append(out, a)
			continue
		}

		err = filepath.WalkDir(a, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				return nil
			}
			lower := strings.ToLower(d.Name())
			if strings.HasSuffix(lower, ".ndjson") || strings.HasSuffix(lower, ".jsonl") ||
				strings.HasSuffix(lower, ".ndjson.gz") || strings.HasSuffix(lower, ".jsonl.gz") {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	// Keep deterministic ordering, especially when walking directories.
	sort.Strings(out)
	return out, nil
}

func processInput(path string, c *counts, opts options) error {
	if path != "-" && opts.printSize {
		sz, err := fileSizeBytes(path)
		if err != nil {
			return err
		}
		c.sizeBytes = sz
		c.hasSize = true
	}

	var expectedLines *uint64
	if path != "-" && (opts.precount || opts.printMD5) {
		if opts.precount {
			n, md5Hex, err := countLinesAndMD5(path)
			if err != nil {
				return err
			}
			expectedLines = &n
			if opts.printMD5 {
				c.md5Hex = md5Hex
			}
			if opts.progress {
				fmt.Fprintf(os.Stderr, "%s: %d lines\n", displayPath(path), n)
			}
		} else if opts.printMD5 {
			md5Hex, err := md5File(path)
			if err != nil {
				return err
			}
			c.md5Hex = md5Hex
		}
	}

	var r io.Reader
	var closer io.Closer

	if path == "-" {
		r = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		r = f
		closer = f

		if strings.HasSuffix(strings.ToLower(path), ".gz") {
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

	// bufio.Scanner is fast and low allocation, but we must increase its token limit.
	// FHIR resources can be large (especially with large extensions).
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 256*1024), 50*1024*1024) // up to 50MB per line

	var lineNum uint64
	lastProgressAt := time.Now()

	for scanner.Scan() {
		line := scanner.Bytes()
		lineNum++
		c.totalLines++

		if opts.progress {
			maybePrintProgress(os.Stderr, displayPath(path), lineNum, expectedLines, &lastProgressAt)
		}

		// Skip empty/whitespace-only lines.
		if len(bytesTrimSpace(line)) == 0 {
			c.emptyLines++
			continue
		}

		var h headerOnly
		if err := json.Unmarshal(line, &h); err != nil {
			c.invalidLines++
			if opts.strict {
				return fmt.Errorf("%s: invalid JSON on line %d: %w", displayPath(path), lineNum, err)
			}
			continue
		}
		if h.ResourceType == "" {
			c.byType["<missing_resourceType>"]++
			continue
		}
		c.byType[h.ResourceType]++
	}
	if err := scanner.Err(); err != nil {
		// Commonly means a line exceeded our max token size.
		return fmt.Errorf("%s: scan error: %w", displayPath(path), err)
	}
	if opts.progress {
		printProgressDone(os.Stderr, displayPath(path), lineNum, expectedLines)
	}
	return nil
}

func maybePrintProgress(w io.Writer, label string, current uint64, expected *uint64, lastAt *time.Time) {
	// Keep progress updates cheap and avoid spamming stderr.
	if current%1000 != 0 && time.Since(*lastAt) < 200*time.Millisecond {
		return
	}
	*lastAt = time.Now()

	if expected == nil || *expected == 0 {
		fmt.Fprintf(w, "\r%s: %d lines", label, current)
		return
	}

	pct := float64(current) / float64(*expected)
	if pct > 1 {
		pct = 1
	}
	barWidth := 30
	filled := int(pct * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("#", filled) + strings.Repeat("-", barWidth-filled)
	fmt.Fprintf(w, "\r%s: [%s] %6.2f%% (%d/%d)", label, bar, pct*100, current, *expected)
}

func printProgressDone(w io.Writer, label string, current uint64, expected *uint64) {
	// Print a final 100% line and newline so subsequent stderr prints don't overwrite.
	if expected != nil && *expected > 0 {
		t := time.Time{}
		// Print one last progress line (even if current != expected).
		maybePrintProgress(w, label, current, expected, &t)
	} else {
		fmt.Fprintf(w, "\r%s: %d lines", label, current)
	}
	fmt.Fprintln(w)
}

// countLines is a quick, streaming line counter (similar to `wc -l`).
// For gzip files it counts lines in the decompressed stream.
func countLinesAndMD5(path string) (uint64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, 1024*1024)
	h := md5.New()
	var lines uint64
	var sawAny bool
	var lastByte byte
	isGzip := strings.HasSuffix(strings.ToLower(path), ".gz")

	// Pass 1:
	//   * always compute md5 over the raw file bytes
	//   * for non-gzip inputs, also count lines in this same pass

	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			sawAny = true
			_, _ = h.Write(buf[:n])
			lastByte = buf[n-1]
			if !isGzip {
				for _, b := range buf[:n] {
					if b == '\n' {
						lines++
					}
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return 0, "", readErr
		}
	}

	md5Hex := hex.EncodeToString(h.Sum(nil))

	if isGzip {
		// Pass 2: count lines in the decompressed stream.
		if _, err := f.Seek(0, 0); err != nil {
			return 0, "", err
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			return 0, "", err
		}
		defer func() { _ = gz.Close() }()

		lines = 0
		sawAny = false
		lastByte = 0
		for {
			n, readErr := gz.Read(buf)
			if n > 0 {
				sawAny = true
				lastByte = buf[n-1]
				for _, b := range buf[:n] {
					if b == '\n' {
						lines++
					}
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return 0, "", readErr
			}
		}
	}

	// If the last line doesn't end with a newline, still count it.
	if sawAny && lastByte != '\n' {
		lines++
	}
	return lines, md5Hex, nil
}

func md5File(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()

	h := md5.New()
	if _, err := io.CopyBuffer(h, f, make([]byte, 1024*1024)); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func fileSizeBytes(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if fi.Size() < 0 {
		return 0, fmt.Errorf("%s: negative file size??", path)
	}
	return uint64(fi.Size()), nil
}

type countRow struct {
	resourceType string
	count        uint64
}

func printCounts(w io.Writer, c *counts, withHeader bool) {
	rows := make([]countRow, 0, len(c.byType))
	for rt, cnt := range c.byType {
		rows = append(rows, countRow{resourceType: rt, count: cnt})
	}

	sort.Slice(rows, func(i, j int) bool {
		if rows[i].count != rows[j].count {
			return rows[i].count > rows[j].count
		}
		return rows[i].resourceType < rows[j].resourceType
	})

	if withHeader {
		fmt.Fprintln(w, "resourceType\tcount")
	}
	var totalCounted uint64
	for _, r := range rows {
		fmt.Fprintf(w, "%s\t%d\n", r.resourceType, r.count)
		totalCounted += r.count
	}
	fmt.Fprintf(w, "TOTAL\t%d\n", totalCounted)
	if c.emptyLines > 0 {
		fmt.Fprintf(w, "EMPTY_LINES\t%d\n", c.emptyLines)
	}
	if c.invalidLines > 0 {
		fmt.Fprintf(w, "INVALID_LINES\t%d\n", c.invalidLines)
	}
	if c.md5Hex != "" {
		fmt.Fprintf(w, "MD5SUM\t%s\n", c.md5Hex)
	}
	if c.hasSize {
		fmt.Fprintf(w, "FILE_SIZE_BYTES\t%d\n", c.sizeBytes)
	}
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

func displayPath(p string) string {
	if p == "-" {
		return "stdin"
	}
	return p
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
