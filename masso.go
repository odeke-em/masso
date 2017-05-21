package masso

import (
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"sync"

	"golang.org/x/crypto/blake2b"
)

type Node struct {
	startByteIndex uint64
	endByteIndex   uint64

	Checksum string      `json:"checksum"`
	Data     interface{} `json:"data,omitempty"`

	Parent     *Node `json:"parent,omitempty"`
	LeftChild  *Node `json:"left_child,omitempty"`
	RightChild *Node `json:"right_child,omitempty"`
}

func (n *Node) StartIndex() uint64 {
	if n == nil {
		return 0
	}
	return n.startByteIndex
}

func (n *Node) EndIndex() uint64 {
	if n == nil {
		return 0
	}
	return n.endByteIndex
}

type MerkleTree struct {
	sync.RWMutex

	root  *Node
	index map[string][]*Node
	_hash hash.Hash
}

func (mk *MerkleTree) BreadthTraverse(fn func(*Node)) {
	if fn == nil {
		return
	}
	mk.root.breadthTraverse(fn)
}

func blake2bHash() hash.Hash {
	h, _ := blake2b.New256(nil)
	return h
}

func (mk *MerkleTree) hash() hash.Hash {
	mk.RLock()
	defer mk.RUnlock()

	if mk == nil || mk._hash == nil {
		return blake2bHash()
	}
	return mk._hash
}

func (mk *MerkleTree) SetHash(h hash.Hash) {
	mk.Lock()
	mk._hash = h
	mk.Unlock()
}

func NewMerkleTree(h hash.Hash) *MerkleTree {
	return &MerkleTree{_hash: h}
}

func ReverseMerklefy(rs io.ReadSeeker, hash hash.Hash, blockSize int64) (*MerkleTree, error) {
	rsr, err := NewReverseSeekReader(rs)
	if err != nil {
		return nil, err
	}
	return Merklefy(rsr, hash, blockSize)
}

func Merklefy(r io.Reader, hash hash.Hash, blockSize int64) (*MerkleTree, error) {
	mk := NewMerkleTree(hash)

	if err := mk.Merklefy(r, blockSize); err != nil {
		return nil, err
	}
	return mk, nil
}

func (mk *MerkleTree) Merklefy(r io.Reader, blockSize int64) error {
	theHash := mk.hash()
	nodes, err := generateZerothLevel(r, blockSize, theHash)
	if err != nil {
		return err
	}

	mk.root = merklefy(nodes, theHash)
	mk.index = mk.root.index()
	return nil
}

var (
	errNoMatchFound  = errors.New("no match found")
	errNotYetIndexed = errors.New("not yet indexed")
)

func (mk *MerkleTree) Lookup(checksum string) ([]*Node, error) {
	mk.RLock()
	defer mk.RUnlock()

	if mk.index == nil {
		return nil, errNotYetIndexed
	}

	return mk.index[checksum], nil
}

var errEmptyChecksum = errors.New("empty checksum")

func (n *Node) consistent() error {
	if n == nil {
		return nil
	}
	if n.Checksum == "" {
		return errEmptyChecksum
	}

	// TODO: Ensure No cycles allowed
	return nil
}

type Index struct {
	// A node checksum can map to more than one node
	internal map[string][]*Node
}

var errNilReader = errors.New("nil reader passed in")

func generateZerothLevel(r io.Reader, blockSize int64, h hash.Hash) ([]*Node, error) {
	if r == nil {
		return nil, errNilReader
	}

	processing := true
	byteIndex := uint64(0)

	slots := make([]*Node, 0, 100) // Arbitrary initial guess

	for processing {
		lr := io.LimitReader(r, blockSize)
		h.Reset()
		n, err := io.Copy(h, lr)
		if n <= 0 {
			break
		}
		endByteIndex := byteIndex + uint64(n)
		slot := &Node{
			Checksum:       fmt.Sprintf("%x", h.Sum(nil)),
			startByteIndex: byteIndex,
			endByteIndex:   endByteIndex,
		}
		slots = append(slots, slot)

		// Now mutate the byteIndex
		byteIndex = endByteIndex

		if err != nil {
			if err == io.EOF {
				processing = false
				break
			}
			return slots, err
		}
	}

	return slots, nil
}

func merklefy(slots []*Node, h hash.Hash) *Node {
	if len(slots) == 0 {
		return nil
	}

	if len(slots) == 1 {
		return slots[0]
	}

	// For each level generate group the ith and (i+1)th nodes
	// to generate the parent
	processing := true
	var parents []*Node

	i := 0
	for processing {
		switch {
		case i >= len(slots): // No more nodes left
			processing = false
			break

		case i+1 == len(slots): // Only one node left
			parents = append(parents, slots[i])
			i += 1

		case i+1 < len(slots): // More than one left
			left, right := slots[i], slots[i+1]
			parent := &Node{
				Checksum: checksumify(left, right, h),
			}

			left.setParent(parent)
			right.setParent(parent)
			parent.setChildren(left, right)
			parents = append(parents, parent)

			i += 2
		}
	}

	// Then for the parents do the same, until we hit the parent
	return merklefy(parents, h)
}

func (n *Node) setParent(parent *Node) {
	if n != nil {
		n.Parent = parent
	}
}

func (n *Node) setChildren(left, right *Node) {
	if n != nil {
		n.LeftChild = left
		n.RightChild = right
		n.startByteIndex = left.StartIndex()
		n.endByteIndex = right.EndIndex()
	}
}

func (n *Node) index() map[string][]*Node {
	if n == nil {
		return nil
	}

	mp := make(map[string][]*Node)
	mp[n.Checksum] = append(mp[n.Checksum], n)
	indices := []map[string][]*Node{
		n.LeftChild.index(),
		n.RightChild.index(),
	}

	for _, index := range indices {
		for checksum, matches := range index {
			mp[checksum] = append(mp[checksum], matches...)
		}
	}

	return mp
}

// checksumify concatenates and then checksums
// the checksums of the left and right nodes.
func checksumify(left, right *Node, h hash.Hash) string {
	if left == nil && right == nil {
		return ""
	}
	h.Reset()
	rawConcatSum := fmt.Sprintf("%s%s", left.checksum(), right.checksum())
	io.WriteString(h, rawConcatSum)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func (n *Node) checksum() string {
	if n == nil {
		return ""
	}
	return n.Checksum
}

func (n *Node) breadthTraverse(fn func(n *Node)) {
	if n == nil {
		return
	}

	fn(n)
	fn(n.LeftChild)
	fn(n.RightChild)

	n.LeftChild.breadthTraverse(fn)
	n.RightChild.breadthTraverse(fn)
}

type ReverseSeekReader struct {
	sync.RWMutex

	pos int64
	rs  io.ReadSeeker

	examined bool

	atFront bool
}

var _ io.Reader = (*ReverseSeekReader)(nil)

func NewReverseSeekReader(rs io.ReadSeeker) (*ReverseSeekReader, error) {
	_, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	rsr := &ReverseSeekReader{rs: rs}
	return rsr, nil
}

func reverse(b []byte) {
	for i := 0; i < len(b)/2; i++ {
		iback := len(b) - i - 1
		front := b[i]
		b[i] = b[iback]
		b[iback] = front
	}
}

func smallestPowOf2(n int64) int64 {
	nearestPowOf2 := math.Floor(math.Log2(float64(n)))
	return int64(math.Pow(2, nearestPowOf2))
}

func minOf(args ...int64) int64 {
	min := args[0]
	for _, arg := range args {
		if arg < min {
			min = arg
		}
	}
	return min
}

func (rsr *ReverseSeekReader) Read(b []byte) (int, error) {
	rsr.Lock()
	defer rsr.Unlock()

	if rsr.atFront {
		return 0, io.EOF
	}

	// examined step is to seek backwards
	// Seeking in powers of 2, because previously
	// noticed that reads not in those powers were failing.
	var sz = int64(len(b))
	if rsr.examined {
		sz = minOf(rsr.pos, int64(len(b)))
	} else {
		rsr.examined = true
	}

	blockSize := smallestPowOf2(sz)
	nSought, _ := rsr.rs.Seek(-1*blockSize, io.SeekCurrent)
	lr := io.LimitReader(rsr.rs, blockSize)
	nRead, err := lr.Read(b)

	if nRead >= 0 {
		b = b[:nRead]
		// Now reverse the bytes
		reverse(b)
	}

	if err != nil {
		return nRead, err
	}

	rsr.pos, _ = rsr.rs.Seek(-1*int64(nRead), io.SeekCurrent)
	if nSought <= 0 {
		rsr.atFront = true
	}

	return len(b), err
}
