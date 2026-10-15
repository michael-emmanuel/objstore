package object

import (
	"strings"
	"testing"
	"time"
)

func TestValidateBucket(t *testing.T) {
	good := []string{"photos", "a-b.c", "abc", strings.Repeat("a", 63)}
	bad := []string{"", "ab", "Photos", "-abc", "abc-", "a..b", "a_b", strings.Repeat("a", 64), "a/b"}
	for _, b := range good {
		if err := ValidateBucket(b); err != nil {
			t.Errorf("ValidateBucket(%q) = %v, want nil", b, err)
		}
	}
	for _, b := range bad {
		if err := ValidateBucket(b); err == nil {
			t.Errorf("ValidateBucket(%q) = nil, want error", b)
		}
	}
}

func TestValidateKey(t *testing.T) {
	// Traversal-looking keys are legal keys: they never touch the filesystem.
	good := []string{"a", "user-123/profile.jpg", "../../etc/passwd", "ünïcode/ключ", strings.Repeat("k", MaxKeyBytes)}
	bad := []string{"", "a\x00b", "line\nbreak", string([]byte{0xff, 0xfe}), strings.Repeat("k", MaxKeyBytes+1)}
	for _, k := range good {
		if err := ValidateKey(k); err != nil {
			t.Errorf("ValidateKey(%q) = %v", k, err)
		}
	}
	for _, k := range bad {
		if err := ValidateKey(k); err == nil {
			t.Errorf("ValidateKey(%q) = nil, want error", k)
		}
	}
}

func TestBlobIDsArePathSafeAndUnambiguous(t *testing.T) {
	id := SegmentBlobID("photos", "../../etc/passwd", "v1", 0)
	if !ValidBlobID(id) {
		t.Fatalf("derived id %q is not a valid blob id", id)
	}
	// Length prefixing prevents ("ab","c") colliding with ("a","bc").
	if SegmentBlobID("ab", "c", "v", 0) == SegmentBlobID("a", "bc", "v", 0) {
		t.Fatal("ambiguous blob id derivation")
	}
	if SegmentBlobID("b", "k", "v", 0) == SegmentBlobID("b", "k", "v", 1) {
		t.Fatal("segments must have distinct ids")
	}
	if PartBlobID("b", "k", "u", 1, "x") == PartBlobID("b", "k", "u", 1, "y") {
		t.Fatal("part retries must have distinct ids")
	}
	for _, bad := range []string{"", "../x", strings.Repeat("A", 32), strings.Repeat("0", 31)} {
		if ValidBlobID(bad) {
			t.Errorf("ValidBlobID(%q) = true", bad)
		}
	}
}

func TestVersionIDsUniqueAndTimeOrderedPrefix(t *testing.T) {
	now := time.Now()
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewVersionID(now)
		if seen[id] || len(id) != 32 {
			t.Fatalf("bad or duplicate id %q", id)
		}
		seen[id] = true
	}
	a := NewVersionID(now)
	b := NewVersionID(now.Add(time.Second))
	if a[:12] >= b[:12] {
		t.Fatalf("time prefix not ordered: %s %s", a, b)
	}
}

func TestCloneIsDeep(t *testing.T) {
	v := Version{Segments: []Segment{{Replicas: []string{"a"}}}, UserMeta: map[string]string{"x": "y"}}
	c := v.Clone()
	c.Segments[0].Replicas[0] = "mutated"
	c.UserMeta["x"] = "mutated"
	if v.Segments[0].Replicas[0] != "a" || v.UserMeta["x"] != "y" {
		t.Fatal("Clone shares state with original")
	}
}

func TestMultipartETag(t *testing.T) {
	d := strings.Repeat("ab", 32)
	e, err := MultipartETag([]string{d, d})
	if err != nil || !strings.HasSuffix(e, "-2") {
		t.Fatalf("got %q, %v", e, err)
	}
	if _, err := MultipartETag([]string{"zz"}); err == nil {
		t.Fatal("expected error for bad digest")
	}
}
