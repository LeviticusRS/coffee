package coffee

import (
    "fmt"
    "strings"
)

const (
    NullTerminator = 0
)

type ByteBuffer struct {
    Bytes  []byte
    Offset int
}

func NewByteBuffer(capacity int) ByteBuffer {
    return ByteBuffer{Bytes: make([]byte, capacity)}
}

func (b *ByteBuffer) check(n int) error {
    if b.Offset+n > len(b.Bytes) {
        return fmt.Errorf(
            "coffee: insufficient number of bytes in buffer to perform this operation (off: %d, n: %d, len: %d)",
            b.Offset, n, len(b.Bytes),
        )
    }
    return nil
}

func (b *ByteBuffer) PutBytes(arr []byte) error {
    if err := b.check(len(arr)); err != nil {
        return err
    }
    copy(b.Bytes[b.Offset:], arr)
    return nil
}

func (b *ByteBuffer) GetUint8() (uint8, error) {
    if err := b.check(1); err != nil {
        return 0, err
    }
    b.Offset += 1
    v := b.Bytes[b.Offset-1]
    return v, nil
}

func (b *ByteBuffer) PutUint8(v uint8) error {
    if err := b.check(1); err != nil {
        return err
    }
    b.Offset += 1
    b.Bytes[b.Offset-1] = uint8(v)
    return nil
}

func (b *ByteBuffer) GetUint16() (uint16, error) {
    if err := b.check(2); err != nil {
        return 0, err
    }
    b.Offset += 2
    v := uint16(b.Bytes[b.Offset-2])<<8 | uint16(b.Bytes[b.Offset-1])
    return v, nil
}

func (b *ByteBuffer) PutUint16(v uint16) error {
    if err := b.check(2); err != nil {
        return err
    }
    b.Offset += 2
    b.Bytes[b.Offset-2] = uint8(v >> 8)
    b.Bytes[b.Offset-1] = uint8(v)
    return nil
}

func (b *ByteBuffer) GetInt32() (int32, error) {
    if err := b.check(4); err != nil {
        return 0, err
    }
    b.Offset += 4
    v := int32(b.Bytes[b.Offset-4])<<24 | int32(b.Bytes[b.Offset-3])<<16 |
        int32(b.Bytes[b.Offset-2])<<8 | int32(b.Bytes[b.Offset-1])
    return v, nil
}

func (b *ByteBuffer) GetUint32() (uint32, error) {
    if err := b.check(4); err != nil {
        return 0, err
    }
    b.Offset += 4
    v := uint32(b.Bytes[b.Offset-4])<<24 | uint32(b.Bytes[b.Offset-3])<<16 |
        uint32(b.Bytes[b.Offset-2])<<8 | uint32(b.Bytes[b.Offset-1])
    return v, nil
}

func (b *ByteBuffer) PutUint32(v uint32) error {
    if err := b.check(4); err != nil {
        return err
    }

    b.Offset += 4
    b.Bytes[b.Offset-4] = uint8(v >> 24)
    b.Bytes[b.Offset-3] = uint8(v >> 16)
    b.Bytes[b.Offset-2] = uint8(v >> 8)
    b.Bytes[b.Offset-1] = uint8(v)
    return nil
}

func (b *ByteBuffer) GetCompactUint32() (uint32, error) {
    if b.Bytes[b.Offset] > 127 {
        v, err := b.GetUint32()
        if err != nil {
            return 0, err
        }
        return v & 0x7fffffff, nil
    } else {
        v, err := b.GetUint16()
        if err != nil {
            return 0, err
        }
        return uint32(v), nil
    }
}

func (b *ByteBuffer) GetUint64() (uint64, error) {
    if err := b.check(8); err != nil {
        return 0, err
    }
    b.Offset += 8
    v := uint64(b.Bytes[b.Offset-8])<<56 | uint64(b.Bytes[b.Offset-7])<<48 |
        uint64(b.Bytes[b.Offset-6])<<40 | uint64(b.Bytes[b.Offset-5])<<32 |
        uint64(b.Bytes[b.Offset-4])<<24 | uint64(b.Bytes[b.Offset-3])<<16 |
        uint64(b.Bytes[b.Offset-2])<<8 | uint64(b.Bytes[b.Offset-1])
    return v, nil
}

func (b *ByteBuffer) PutUint64(v uint64) error {
    if err := b.check(8); err != nil {
        return err
    }
    b.Offset += 8
    b.Bytes[b.Offset-8] = uint8(v >> 56)
    b.Bytes[b.Offset-7] = uint8(v >> 48)
    b.Bytes[b.Offset-6] = uint8(v >> 40)
    b.Bytes[b.Offset-5] = uint8(v >> 32)
    b.Bytes[b.Offset-4] = uint8(v >> 24)
    b.Bytes[b.Offset-3] = uint8(v >> 16)
    b.Bytes[b.Offset-2] = uint8(v >> 8)
    b.Bytes[b.Offset-1] = uint8(v)
    return nil
}

func (b *ByteBuffer) GetCString() (string, error) {
    var s strings.Builder
    for {
        if err := b.check(1); err != nil {
            return "", err
        }

        value, _ := b.GetUint8()
        if value == 0 {
            break
        }

        s.WriteByte(value)
    }

    return s.String(), nil
}

func (b *ByteBuffer) PutCString(v string) error {
    if err := b.check(len(v) + 1); err != nil {
        return err
    }

    for i := 0; i < len(v); i++ {
        _ = b.PutUint8(v[i])
    }

    _ = b.PutUint8(NullTerminator)

    return nil
}

func (b *ByteBuffer) PutBool(v bool) error {
    if v {
        return b.PutUint8(1)
    }
    return b.PutUint8(0)
}

func (b *ByteBuffer) GetBool() (bool, error) {
    v, err := b.GetUint8()
    if err != nil {
        return false, err
    }
    return v == 1, nil
}
