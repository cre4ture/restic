package filechunker

import (
	"log"
)

type ChunkerI interface {
	Next() (ChunkI, error)
}

type ChunkI interface {
	PcHash() [32]byte // precomputed hash or zero-byte filled value to indicate missing pre computed hash
	Size() uint64
	Data() ([]byte, error) // should fetch data lazily on first call
	Release()
}

func ConvenientData(chunk ChunkI) []byte {
	data, err := chunk.Data()
	if err != nil {
		log.Panicf("unable to fetch data from chunk: %v", err)
	}
	return data
}

type RawDataChunk struct {
	buf  []byte
	hash [32]byte
}

func NewRawDataChunk(buf []byte) *RawDataChunk {
	return &RawDataChunk{
		buf:  buf,
		hash: [32]byte{}, // return zero-id to signal that it needs to be computed
	}
}

func NewRawDataChunkWithPreComputedHash(buf []byte, hash [32]byte) *RawDataChunk {
	return &RawDataChunk{
		buf:  buf,
		hash: hash,
	}
}

// Data implements ChunkI.
func (r *RawDataChunk) Data() ([]byte, error) {
	return r.buf, nil
}

// Hash implements ChunkI.
func (r *RawDataChunk) PcHash() [32]byte {
	return r.hash
}

// Release implements ChunkI.
func (r *RawDataChunk) Release() {
	// ignore
}

// Size implements ChunkI.
func (r *RawDataChunk) Size() uint64 {
	return uint64(len(r.buf))
}
