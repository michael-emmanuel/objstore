package api

import "testing"

func TestParseClientRange(t *testing.T) {
	cases := []struct {
		h             string
		size          int64
		start, length int64
		partial, err  bool
	}{
		{"", 100, 0, 100, false, false},
		{"bytes=0-9", 100, 0, 10, true, false},
		{"bytes=90-", 100, 90, 10, true, false},
		{"bytes=-10", 100, 90, 10, true, false},
		{"bytes=-500", 100, 0, 100, true, false},
		{"bytes=50-1000", 100, 50, 50, true, false},
		{"bytes=100-", 100, 0, 0, false, true},
		{"bytes=0-1,5-6", 100, 0, 100, false, false}, // multi-range ignored
		{"bytes=9-1", 100, 0, 100, false, false},     // invalid ignored
		{"items=0-1", 100, 0, 100, false, false},
		{"bytes=0-0", 0, 0, 0, false, true},
	}
	for _, c := range cases {
		s, l, p, err := parseClientRange(c.h, c.size)
		if (err != nil) != c.err || (err == nil && (s != c.start || l != c.length || p != c.partial)) {
			t.Errorf("%q/%d: got %d,%d,%v,%v", c.h, c.size, s, l, p, err)
		}
	}
}

func TestSplitObjectPath(t *testing.T) {
	cases := []struct {
		in, bucket, kind, key string
		ok                    bool
	}{
		{"/v1/buckets/photos/objects/user-123/profile.jpg", "photos", "objects", "user-123/profile.jpg", true},
		{"/v1/buckets/photos/objects/a/../b", "photos", "objects", "a/../b", true},
		{"/v1/buckets/photos/objects/a//b", "photos", "objects", "a//b", true},
		{"/v1/buckets/photos/objects/sp%20ace%3Fq", "photos", "objects", "sp ace?q", true},
		{"/v1/buckets/photos/versions/k", "photos", "versions", "k", true},
		{"/v1/buckets/photos/objects", "", "", "", false},
		{"/v1/buckets/photos/objects/", "", "", "", false},
		{"/v1/buckets/photos/multipart", "", "", "", false},
		{"/admin/cluster", "", "", "", false},
	}
	for _, c := range cases {
		b, k, key, ok := splitObjectPath(c.in)
		if ok != c.ok || (ok && (b != c.bucket || k != c.kind || key != c.key)) {
			t.Errorf("%s: got %q %q %q %v", c.in, b, k, key, ok)
		}
	}
}
