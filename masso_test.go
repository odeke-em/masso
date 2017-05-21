package masso_test

import (
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"golang.org/x/crypto/blake2b"

	"github.com/odeke-em/masso"
)

type index struct {
	startIndex, endIndex int64
}

func fRepeatString(seq string, count int) io.Reader {
	return strings.NewReader(strings.Repeat(seq, count))
}

func blake2bHash() hash.Hash {
	h, _ := blake2b.New256(nil)
	return h
}

func blake2bChecksumOf(r io.Reader) string {
	h := blake2bHash()
	io.Copy(h, r)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TestMerklefy(t *testing.T) {
	tests := [...]struct {
		r         io.Reader
		blockSize int64
		wantErr   bool

		mustPassLookups map[string][]*index
		mustFailLookups []string
	}{
		0: {
			r: io.MultiReader(
				fRepeatString("a", 10),
				fRepeatString("b", 15),
				fRepeatString("c", 15),
				fRepeatString("de", 40),
				fRepeatString("a", 10),
			),
			blockSize: 10,
			mustPassLookups: map[string][]*index{
				blake2bChecksumOf(fRepeatString("a", 10)): {&index{0, 10}, &index{120, 130}},
				blake2bChecksumOf(fRepeatString("de", 5)): {
					&index{40, 50},
					&index{50, 60},
					&index{60, 70},
					&index{70, 80},
					&index{80, 90},
					&index{90, 100},
					&index{100, 110},
					&index{110, 120},
				},
				blake2bChecksumOf(io.MultiReader(
					fRepeatString("b", 5),
					fRepeatString("c", 5),
				)): {&index{20, 30}},
			},
			mustFailLookups: []string{
				blake2bChecksumOf(fRepeatString("a", 12)),
				blake2bChecksumOf(fRepeatString("X", 10)),
			},
		},
	}

	hash := blake2bHash()
	for i, tt := range tests {
		tree, err := masso.Merklefy(tt.r, hash, tt.blockSize)
		if tt.wantErr {
			if err != nil {
				t.Errorf("#%d: expecting non-nil error")
			}
			continue
		}

		if err != nil {
			t.Errorf("#%d: err: %v", i, err)
			continue
		}

		for checksum, wantIndices := range tt.mustPassLookups {
			nodeMatches, err := tree.Lookup(checksum)
			if err != nil {
				t.Errorf("#%d Lookup: checksum: %q %v", i, checksum, err)
				continue
			}

			if got, want := len(nodeMatches), len(wantIndices); got != want {
				t.Errorf("#%d gotLen: %d wantLen: %d checksum: %q", i, got, want, checksum)
			}

			translatedIndex := make(map[string]bool)
			for _, match := range nodeMatches {
				key := fmt.Sprintf("%d%d", match.StartIndex(), match.EndIndex())
				translatedIndex[key] = true
			}

			// Now for the lookup
			for j, index := range wantIndices {
				key := fmt.Sprintf("%d%d", index.startIndex, index.endIndex)
				_, found := translatedIndex[key]
				if !found {
					t.Errorf("(%d): Lookup:: value: #%d %q was not found. Index: %#v", i, j, key, translatedIndex)
				}
			}
		}
	}
}

func TestReverseSeekReader(t *testing.T) {
	f, err := os.Open("./testdata/foxtrot.txt")
	if err != nil {
		t.Fatal(err)
	}
	fi, _ := f.Stat()

	rsr, err := masso.NewReverseSeekReader(f)
	if err != nil {
		t.Fatal(err)
	}

	n, _ := io.Copy(ioutil.Discard, rsr)
	if got, want := n, fi.Size(); got != want {
		t.Errorf("got = %v, want = %v", got, want)
	}
}
