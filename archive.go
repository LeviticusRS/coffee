package coffee

import (
    "bytes"
    "compress/gzip"
    "encoding/binary"
    "fmt"
    "github.com/dsnet/compress/bzip2"
    "io"
)

const (
    UncompressedHeaderLength = 5
    CompressedHeaderLength   = 9
)

var (
    bz2Header = []byte("BZh9")
)

type Compression uint8

const (
    Uncompressed Compression = 0
    Bzip2        Compression = 1
    Gzip         Compression = 2
)

func (c Compression) reader(b []byte) (io.Reader, error) {
    length := binary.BigEndian.Uint32(b[1:])
    switch c {
    case Uncompressed:
        return bytes.NewReader(b[UncompressedHeaderLength : UncompressedHeaderLength+length]), nil
    case Bzip2:
        return bzip2.NewReader(
            bytes.NewReader(
                append(bz2Header, b[CompressedHeaderLength:CompressedHeaderLength+length]...)), &bzip2.ReaderConfig{})
    case Gzip:
        return gzip.NewReader(bytes.NewReader(b[CompressedHeaderLength : CompressedHeaderLength+length]))
    default:
        return nil, fmt.Errorf("asset: unsupported compression %d", c)
    }
}

// Gets the uncompressed length of an archive. This function expects the entire archive (header and payload).
func (c Compression) uncompressedLength(b []byte) (uint32, error) {
    switch c {
    case Uncompressed:
        return binary.BigEndian.Uint32(b[1:]), nil
    case Bzip2, Gzip:
        return binary.BigEndian.Uint32(b[UncompressedHeaderLength:]), nil
    default:
        return 0, fmt.Errorf("asset: unsupported compression %d", c)
    }
}

func DecompressArchive(b []byte) ([]byte, error) {
    compression := Compression(b[0])

    r, err := compression.reader(b)
    if err != nil {
        return nil, err
    }

    length, _ := compression.uncompressedLength(b)
    unpacked := make([]byte, length)

    if _, err := io.ReadAtLeast(r, unpacked, len(unpacked)); err != nil {
        return nil, err
    }

    return unpacked, nil
}

func CompressArchive(compression Compression, b []byte) ([]byte, error) {
    switch compression {
    case Uncompressed:
        packed := make([]byte, UncompressedHeaderLength+len(b))
        buf := ByteBuffer{Bytes: packed}
        _ = buf.PutUint8(byte(compression))
        _ = buf.PutUint32(uint32(len(b)))
        _ = buf.PutBytes(b)
        return packed, nil
    default:
        // TODO
        return nil, fmt.Errorf("asset: unsupported compression %d", compression)
    }
}

// Trims an archive of any extra bytes. For example the client appends two extra bytes at the end of an archive in the
// cache to store the version.
func TrimArchive(b []byte) ([]byte, error) {
    compression := Compression(b[0])
    length := binary.BigEndian.Uint32(b[1:])
    switch compression {
    case Uncompressed:
        return b[:UncompressedHeaderLength+length], nil
    case Bzip2, Gzip:
        return b[:CompressedHeaderLength+length], nil
    default:
        return nil, fmt.Errorf("asset: unsupported compression %d", compression)
    }
}

