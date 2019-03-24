package coffee

func UnpackGroup(b []byte, ids []uint16) (map[uint16][]byte, error) {
    files := make(map[uint16][]byte, len(ids))

    buf := ByteBuffer{Bytes: b, Offset: len(b)-1}

    p, err := buf.GetUint8()
    if err != nil {
        return nil, err
    }

    parts := int(p)

    footerStart := buf.Offset - 1 - parts * len(ids) * 4
    buf.Offset = footerStart

    offsets := make([]int32, len(ids))

    for part := 0; part < parts; part++ {
        counter := int32(0)

        for entry := 0; entry < len(ids); entry++ {
            delta, _ := buf.GetInt32()
            counter += delta
            offsets[entry] += counter
        }
    }

    for i := 0; i < len(ids); i++ {
        files[ids[i]] = make([]byte, offsets[i])
        offsets[i] = 0
    }

    buf.Offset = footerStart

    offset := int32(0)

    for part := 0; part < parts; part++ {
        counter := int32(0)

        for entry := 0; entry < len(ids); entry++ {
            delta, _ := buf.GetInt32()
            counter += delta
            copy(files[ids[entry]][offsets[entry]:], b[offset:offset+counter])
            offsets[entry] += counter
            offset += counter
        }
    }

    return files, nil
}
