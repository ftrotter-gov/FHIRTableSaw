package fasttools

import "testing"

func TestNormalizeStates_PreservesOrderAndDedupes(t *testing.T) {
	states, key, err := normalizeStates([]string{"wy", "RI", "wy", "  ca  "})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if key != "WY-RI-CA" {
		t.Fatalf("expected key WY-RI-CA, got %q", key)
	}
	if !states["WY"] || !states["RI"] || !states["CA"] {
		t.Fatalf("missing expected states in map: %+v", states)
	}
}

func TestParseReference(t *testing.T) {
	id, ok := parseReference("Organization/abc", "Organization")
	if !ok || id != "abc" {
		t.Fatalf("expected abc, ok=true got %q ok=%v", id, ok)
	}
	_, ok = parseReference("Location/123", "Organization")
	if ok {
		t.Fatalf("expected type mismatch to fail")
	}
	id, ok = parseReference("https://example.org/fhir/Endpoint/xyz", "Endpoint")
	if !ok || id != "xyz" {
		t.Fatalf("expected xyz, ok=true got %q ok=%v", id, ok)
	}
}

func TestParseLocationIDAndState_ObjectAndArray(t *testing.T) {
	id, st, ok, err := parseLocationIDAndState([]byte(`{"resourceType":"Location","id":"l1","address":{"state":"wy"}}`))
	if err != nil || !ok {
		t.Fatalf("expected ok, err=nil got ok=%v err=%v", ok, err)
	}
	if id != "l1" || st != "WY" {
		t.Fatalf("expected l1/WY got %q/%q", id, st)
	}

	id, st, ok, err = parseLocationIDAndState([]byte(`{"id":"l2","address":[{"state":""},{"state":"ri"}]}`))
	if err != nil || !ok {
		t.Fatalf("expected ok, err=nil got ok=%v err=%v", ok, err)
	}
	if id != "l2" || st != "RI" {
		t.Fatalf("expected l2/RI got %q/%q", id, st)
	}
}
