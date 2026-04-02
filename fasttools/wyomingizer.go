// Package fasttools contains small utilities for working with FHIR NDJSON data.
package fasttools

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// WyomingizeOptions configures the extraction of a state-bounded subset.
//
// This intentionally supports only a small set of resource types for now:
//   - Location
//   - Organization
//   - OrganizationAffiliation
//   - PractitionerRole
//   - Endpoint
type WyomingizeOptions struct {
	InputDir  string
	OutputDir string

	// States is the list of state codes (e.g. ["WY","RI"]). These are matched against
	// Location.address.state (case-insensitive).
	States []string

	// Overwrite controls whether output files may already exist.
	Overwrite bool

	// Verbose prints progress information to stderr.
	Verbose bool
}

type WyomingizeStats struct {
	StatesKey string

	LocationsWritten                uint64
	OrganizationsWritten            uint64
	OrganizationAffiliationsWritten uint64
	PractitionerRolesWritten        uint64
	EndpointsWritten                uint64
	InvalidJSONLines                uint64
}

// Wyomingize reads NDJSON shards in opts.InputDir and writes a smaller subset to opts.OutputDir.
//
// The selection rules are intentionally bounded:
//  1. Seed Locations whose Location.address.state is in opts.States.
//  2. Include Organizations / PractitionerRoles / OrganizationAffiliations that reference
//     any seeded Location.
//  3. Include one more hop:
//     - Organizations referenced by included PractitionerRoles/OrganizationAffiliations
//     - Endpoints referenced by any included Organization or PractitionerRole
//  4. “Bridge completion”:
//     - Include any PractitionerRole/OrganizationAffiliation that references any included
//     Organization (even if it does not reference a seeded Location).
//     - This does NOT further expand the graph.
func Wyomingize(opts WyomingizeOptions) (WyomingizeStats, error) {
	var st WyomingizeStats

	states, statesKey, err := normalizeStates(opts.States)
	if err != nil {
		return st, err
	}
	st.StatesKey = statesKey

	if strings.TrimSpace(opts.InputDir) == "" {
		return st, fmt.Errorf("input_dir is required")
	}
	if strings.TrimSpace(opts.OutputDir) == "" {
		return st, fmt.Errorf("output_dir is required")
	}

	inputs, err := discoverInputs(opts.InputDir)
	if err != nil {
		return st, err
	}

	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return st, err
	}

	writers, err := newWriters(opts.OutputDir, statesKey, opts.Overwrite)
	if err != nil {
		return st, err
	}
	defer func() { _ = writers.Close() }()

	locationIDs := makeStringSet(1024)
	orgIDs := makeStringSet(1024)
	roleIDs := makeStringSet(1024)
	affIDs := makeStringSet(1024)
	endpointIDs := makeStringSet(1024)
	orgSecondHopIDs := makeStringSet(1024)

	// PASS 1: seed Locations by state.
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "pass1: seeding locations by state (%s)\n", statesKey)
	}
	for _, path := range inputs.locations {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, state, ok, err := parseLocationIDAndState(line)
			if err != nil {
				return err
			}
			if !ok || !states[state] {
				return nil
			}
			locationIDs.add(id)
			if writers.locations.WriteOnce(id, line) {
				st.LocationsWritten++
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("locations scan %s: %w", path, err)
		}
	}

	// PASS 2: resources directly connected to seeded locations.
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "pass2: selecting org/role/aff directly connected to %d locations\n", locationIDs.len())
	}

	for _, path := range inputs.organizations {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, ok, err := parseID(line)
			if err != nil || !ok {
				return err
			}
			locRefs, err := extractRefIDsFromKeys(line, "Location", []string{"location"})
			if err != nil {
				return err
			}
			if !anyInSet(locRefs, locationIDs) {
				return nil
			}
			orgIDs.add(id)
			if writers.organizations.WriteOnce(id, line) {
				st.OrganizationsWritten++
			}
			endRefs, err := extractRefIDsFromKeys(line, "Endpoint", []string{"endpoint"})
			if err != nil {
				return err
			}
			for _, eid := range endRefs {
				endpointIDs.add(eid)
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("organizations scan %s: %w", path, err)
		}
	}

	for _, path := range inputs.practitionerRoles {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, ok, err := parseID(line)
			if err != nil || !ok {
				return err
			}
			locRefs, err := extractRefIDsFromKeys(line, "Location", []string{"location"})
			if err != nil {
				return err
			}
			if !anyInSet(locRefs, locationIDs) {
				return nil
			}

			roleIDs.add(id)
			if writers.practitionerRoles.WriteOnce(id, line) {
				st.PractitionerRolesWritten++
			}

			orgRefs, err := extractRefIDsFromKeys(line, "Organization", []string{"organization"})
			if err != nil {
				return err
			}
			for _, oid := range orgRefs {
				orgSecondHopIDs.add(oid)
			}

			endRefs, err := extractRefIDsFromKeys(line, "Endpoint", []string{"endpoint"})
			if err != nil {
				return err
			}
			for _, eid := range endRefs {
				endpointIDs.add(eid)
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("practitionerRole scan %s: %w", path, err)
		}
	}

	affLocationKeys := []string{"location"}
	for _, path := range inputs.organizationAffiliations {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, ok, err := parseID(line)
			if err != nil || !ok {
				return err
			}
			locRefs, err := extractRefIDsFromKeys(line, "Location", affLocationKeys)
			if err != nil {
				return err
			}
			if !anyInSet(locRefs, locationIDs) {
				return nil
			}

			affIDs.add(id)
			if writers.organizationAffiliations.WriteOnce(id, line) {
				st.OrganizationAffiliationsWritten++
			}

			orgRefs, err := extractRefIDsFromKeys(line, "Organization", []string{"organization", "participatingOrganization"})
			if err != nil {
				return err
			}
			for _, oid := range orgRefs {
				orgSecondHopIDs.add(oid)
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("organizationAffiliation scan %s: %w", path, err)
		}
	}

	// PASS 3: second hop orgs + bridge completion + endpoints.
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "pass3: adding %d second-hop organizations and completing bridge entities\n", orgSecondHopIDs.len())
	}

	for _, path := range inputs.organizations {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, ok, err := parseID(line)
			if err != nil || !ok {
				return err
			}
			if !orgSecondHopIDs.has(id) {
				return nil
			}
			if !orgIDs.has(id) {
				orgIDs.add(id)
				if writers.organizations.WriteOnce(id, line) {
					st.OrganizationsWritten++
				}
			}
			endRefs, err := extractRefIDsFromKeys(line, "Endpoint", []string{"endpoint"})
			if err != nil {
				return err
			}
			for _, eid := range endRefs {
				endpointIDs.add(eid)
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("organizations hop2 scan %s: %w", path, err)
		}
	}

	for _, path := range inputs.practitionerRoles {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, ok, err := parseID(line)
			if err != nil || !ok || roleIDs.has(id) {
				return err
			}
			orgRefs, err := extractRefIDsFromKeys(line, "Organization", []string{"organization"})
			if err != nil {
				return err
			}
			if !anyInSet(orgRefs, orgIDs) {
				return nil
			}
			roleIDs.add(id)
			if writers.practitionerRoles.WriteOnce(id, line) {
				st.PractitionerRolesWritten++
			}
			endRefs, err := extractRefIDsFromKeys(line, "Endpoint", []string{"endpoint"})
			if err != nil {
				return err
			}
			for _, eid := range endRefs {
				endpointIDs.add(eid)
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("practitionerRole bridge scan %s: %w", path, err)
		}
	}

	for _, path := range inputs.organizationAffiliations {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, ok, err := parseID(line)
			if err != nil || !ok || affIDs.has(id) {
				return err
			}
			orgRefs, err := extractRefIDsFromKeys(line, "Organization", []string{"organization", "participatingOrganization"})
			if err != nil {
				return err
			}
			if !anyInSet(orgRefs, orgIDs) {
				return nil
			}
			affIDs.add(id)
			if writers.organizationAffiliations.WriteOnce(id, line) {
				st.OrganizationAffiliationsWritten++
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("organizationAffiliation bridge scan %s: %w", path, err)
		}
	}

	for _, path := range inputs.endpoints {
		inv, err := scanFileLines(path, func(line []byte) error {
			id, ok, err := parseID(line)
			if err != nil || !ok {
				return err
			}
			include := endpointIDs.has(id)
			if !include {
				orgRefs, err := extractRefIDsFromKeys(line, "Organization", []string{"managingOrganization"})
				if err != nil {
					return err
				}
				include = anyInSet(orgRefs, orgIDs)
			}
			if !include {
				return nil
			}
			if writers.endpoints.WriteOnce(id, line) {
				st.EndpointsWritten++
			}
			return nil
		})
		st.InvalidJSONLines += inv
		if err != nil {
			return st, fmt.Errorf("endpoints scan %s: %w", path, err)
		}
	}

	if err := writers.Flush(); err != nil {
		return st, err
	}

	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "done: wrote Location=%d Organization=%d PractitionerRole=%d OrganizationAffiliation=%d Endpoint=%d (invalid_json=%d)\n",
			st.LocationsWritten, st.OrganizationsWritten, st.PractitionerRolesWritten, st.OrganizationAffiliationsWritten, st.EndpointsWritten, st.InvalidJSONLines)
	}
	return st, nil
}

type inputFiles struct {
	locations                []string
	organizations            []string
	organizationAffiliations []string
	practitionerRoles        []string
	endpoints                []string
}

func discoverInputs(inputDir string) (inputFiles, error) {
	var out inputFiles
	var err error
	out.locations, err = findShardFiles(inputDir, "Location")
	if err != nil {
		return out, err
	}
	out.organizations, err = findShardFiles(inputDir, "Organization")
	if err != nil {
		return out, err
	}
	out.organizationAffiliations, err = findShardFiles(inputDir, "OrganizationAffiliation")
	if err != nil {
		return out, err
	}
	out.practitionerRoles, err = findShardFiles(inputDir, "PractitionerRole")
	if err != nil {
		return out, err
	}
	out.endpoints, err = findShardFiles(inputDir, "Endpoint")
	if err != nil {
		return out, err
	}
	return out, nil
}

func findShardFiles(dir, prefix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	pfx := strings.ToLower(prefix)
	var out []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		lower := strings.ToLower(e.Name())
		if !strings.HasPrefix(lower, pfx) {
			continue
		}
		if strings.HasSuffix(lower, ".gz") {
			continue
		}
		out = append(out, filepath.Join(dir, e.Name()))
	}
	sort.Strings(out)
	return out, nil
}

func scanFileLines(path string, fn func(line []byte) error) (invalidJSON uint64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 256*1024), 50*1024*1024)
	for scanner.Scan() {
		trimmed := bytesTrimSpace(scanner.Bytes())
		if len(trimmed) == 0 {
			continue
		}
		if err := fn(trimmed); err != nil {
			var je *json.SyntaxError
			if errors.As(err, &je) {
				invalidJSON++
				continue
			}
			return invalidJSON, err
		}
	}
	if err := scanner.Err(); err != nil {
		return invalidJSON, err
	}
	return invalidJSON, nil
}

func parseID(line []byte) (id string, ok bool, err error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		return "", false, err
	}
	raw, ok := m["id"]
	if !ok {
		return "", false, nil
	}
	if err := json.Unmarshal(raw, &id); err != nil {
		return "", false, err
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return "", false, nil
	}
	return id, true, nil
}

func parseLocationIDAndState(line []byte) (id string, state string, ok bool, err error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		return "", "", false, err
	}
	if raw, ok := m["id"]; ok {
		if err := json.Unmarshal(raw, &id); err != nil {
			return "", "", false, err
		}
		id = strings.TrimSpace(id)
	}
	if id == "" {
		return "", "", false, nil
	}
	addrRaw, ok := m["address"]
	if !ok {
		return "", "", false, nil
	}

	var addrObj struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(addrRaw, &addrObj); err == nil {
		state = strings.ToUpper(strings.TrimSpace(addrObj.State))
		if state == "" {
			return "", "", false, nil
		}
		return id, state, true, nil
	}
	var addrArr []struct {
		State string `json:"state"`
	}
	if err := json.Unmarshal(addrRaw, &addrArr); err != nil {
		return "", "", false, nil
	}
	for _, a := range addrArr {
		state = strings.ToUpper(strings.TrimSpace(a.State))
		if state != "" {
			return id, state, true, nil
		}
	}
	return "", "", false, nil
}

func extractRefIDsFromKeys(line []byte, wantType string, keys []string) ([]string, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(line, &m); err != nil {
		return nil, err
	}
	var out []string
	for _, k := range keys {
		raw, ok := m[k]
		if !ok {
			continue
		}
		ids, err := extractRefIDs(raw, wantType)
		if err != nil {
			return nil, err
		}
		out = append(out, ids...)
	}
	return out, nil
}

func extractRefIDs(raw json.RawMessage, wantType string) ([]string, error) {
	raw = bytesTrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	switch raw[0] {
	case '[':
		var arr []json.RawMessage
		if err := json.Unmarshal(raw, &arr); err != nil {
			return nil, err
		}
		var out []string
		for _, item := range arr {
			ids, err := extractRefIDs(item, wantType)
			if err != nil {
				return nil, err
			}
			out = append(out, ids...)
		}
		return out, nil
	case '{':
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, err
		}
		if refRaw, ok := obj["reference"]; ok {
			var refStr string
			if err := json.Unmarshal(refRaw, &refStr); err != nil {
				return nil, err
			}
			if id, ok := parseReference(refStr, wantType); ok {
				return []string{id}, nil
			}
			return nil, nil
		}
		return nil, nil
	case '"':
		var refStr string
		if err := json.Unmarshal(raw, &refStr); err != nil {
			return nil, err
		}
		if id, ok := parseReference(refStr, wantType); ok {
			return []string{id}, nil
		}
		return nil, nil
	default:
		return nil, nil
	}
}

func parseReference(ref string, wantType string) (id string, ok bool) {
	ref = strings.TrimSpace(ref)
	if ref == "" || strings.HasPrefix(ref, "#") {
		return "", false
	}
	if i := strings.IndexAny(ref, "?#"); i >= 0 {
		ref = ref[:i]
	}
	parts := strings.Split(ref, "/")
	if len(parts) < 2 {
		return "", false
	}
	rt := parts[len(parts)-2]
	id = parts[len(parts)-1]
	if id == "" {
		return "", false
	}
	if wantType != "" && !strings.EqualFold(rt, wantType) {
		return "", false
	}
	return id, true
}

type writers struct {
	locations                *ndjsonWriter
	organizations            *ndjsonWriter
	organizationAffiliations *ndjsonWriter
	practitionerRoles        *ndjsonWriter
	endpoints                *ndjsonWriter
}

func newWriters(outDir, statesKey string, overwrite bool) (*writers, error) {
	open := func(rt string) (*ndjsonWriter, error) {
		path := filepath.Join(outDir, fmt.Sprintf("%s.%s.ndjson", rt, statesKey))
		return newNDJSONWriter(path, overwrite)
	}

	loc, err := open("Location")
	if err != nil {
		return nil, err
	}
	org, err := open("Organization")
	if err != nil {
		_ = loc.Close()
		return nil, err
	}
	aff, err := open("OrganizationAffiliation")
	if err != nil {
		_ = loc.Close()
		_ = org.Close()
		return nil, err
	}
	role, err := open("PractitionerRole")
	if err != nil {
		_ = loc.Close()
		_ = org.Close()
		_ = aff.Close()
		return nil, err
	}
	ep, err := open("Endpoint")
	if err != nil {
		_ = loc.Close()
		_ = org.Close()
		_ = aff.Close()
		_ = role.Close()
		return nil, err
	}

	return &writers{locations: loc, organizations: org, organizationAffiliations: aff, practitionerRoles: role, endpoints: ep}, nil
}

func (w *writers) Flush() error {
	return errors.Join(
		w.locations.Flush(),
		w.organizations.Flush(),
		w.organizationAffiliations.Flush(),
		w.practitionerRoles.Flush(),
		w.endpoints.Flush(),
	)
}

func (w *writers) Close() error {
	return errors.Join(
		w.locations.Close(),
		w.organizations.Close(),
		w.organizationAffiliations.Close(),
		w.practitionerRoles.Close(),
		w.endpoints.Close(),
	)
}

type ndjsonWriter struct {
	path    string
	f       *os.File
	bw      *bufio.Writer
	written stringSet
}

func newNDJSONWriter(path string, overwrite bool) (*ndjsonWriter, error) {
	if !overwrite {
		if _, err := os.Stat(path); err == nil {
			return nil, fmt.Errorf("output exists (use --overwrite): %s", path)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	return &ndjsonWriter{path: path, f: f, bw: bufio.NewWriterSize(f, 1024*1024), written: makeStringSet(1024)}, nil
}

func (w *ndjsonWriter) WriteOnce(id string, line []byte) bool {
	if w.written.has(id) {
		return false
	}
	w.written.add(id)
	_, _ = w.bw.Write(line)
	_ = w.bw.WriteByte('\n')
	return true
}

func (w *ndjsonWriter) Flush() error {
	if w == nil {
		return nil
	}
	return w.bw.Flush()
}

func (w *ndjsonWriter) Close() error {
	if w == nil {
		return nil
	}
	if err := w.Flush(); err != nil {
		_ = w.f.Close()
		return err
	}
	return w.f.Close()
}

type stringSet map[string]struct{}

func makeStringSet(capacity int) stringSet {
	if capacity <= 0 {
		capacity = 8
	}
	return make(map[string]struct{}, capacity)
}

func (s stringSet) add(v string) { s[v] = struct{}{} }
func (s stringSet) has(v string) bool {
	_, ok := s[v]
	return ok
}
func (s stringSet) len() int { return len(s) }

func anyInSet(ids []string, s stringSet) bool {
	for _, id := range ids {
		if s.has(id) {
			return true
		}
	}
	return false
}

func normalizeStates(in []string) (map[string]bool, string, error) {
	if len(in) == 0 {
		return nil, "", fmt.Errorf("states is required")
	}
	states := make(map[string]bool, len(in))
	ordered := make([]string, 0, len(in))
	for _, s := range in {
		ss := strings.ToUpper(strings.TrimSpace(s))
		if ss == "" {
			continue
		}
		if !states[ss] {
			states[ss] = true
			ordered = append(ordered, ss)
		}
	}
	if len(ordered) == 0 {
		return nil, "", fmt.Errorf("states is required")
	}
	return states, strings.Join(ordered, "-"), nil
}

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
