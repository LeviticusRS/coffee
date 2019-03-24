package coffee

import (
    "fmt"
    "sort"
)

const (
    NamedFlag = 0x1

    MinimumFormat = 5
    MaximumFormat = 6
)


type GroupEntry struct {
    Files    map[uint16]*FileEntry
    Name     uint32
    Checksum uint32
    Version  uint32
}

// Ids returns a newly allocated slice of all of the file entry ids sorted by ascending value.
func (e *GroupEntry) Ids() []uint16 {
    ids := make([]uint16, 0, len(e.Files))
    for k := range e.Files {
        ids = append(ids, k)
    }
    sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
    return ids
}

type FileEntry struct {
    Name uint32
}

type Manifest struct {
    Groups  map[uint16]*GroupEntry
    Version uint32
}

func DecodeManifest(b []byte) (*Manifest, error) {
    buf := ByteBuffer{Bytes: b}

    format, err := buf.GetUint8()
    if err != nil {
        return nil, err
    }

    if format < MinimumFormat || format > MaximumFormat {
        return nil, fmt.Errorf("coffee: unsupported manifest format - %d", format)
    }

    manifest := new(Manifest)

    if format >= 6 {
        if manifest.Version, err = buf.GetUint32(); err != nil {
            return nil, err
        }
    }

    var flags uint8
    if flags, err = buf.GetUint8(); err != nil {
        return nil, err
    }

    var count uint16
    if count, err = buf.GetUint16(); err != nil {
        return nil, err
    }

    manifest.Groups = make(map[uint16]*GroupEntry, count)

    ids := make([]uint16, count)

    id := uint16(0)
    for i := 0; i < len(ids); i++ {
        skip, err := buf.GetUint16()
        if err != nil {
            return nil, err
        }
        id += skip

        manifest.Groups[id] = new(GroupEntry)
        ids[i] = id
    }

    if flags&NamedFlag != 0 {
        for i := 0; i < len(ids); i++ {
            var name uint32
            if name, err = buf.GetUint32(); err != nil {
                return nil, err
            }
            manifest.Groups[ids[i]].Name = name
        }
    }

    for i := 0; i < len(ids); i++ {
        var checksum uint32
        if checksum, err = buf.GetUint32(); err != nil {
            return nil, err
        }
        manifest.Groups[ids[i]].Checksum = checksum
    }

    for i := 0; i < len(ids); i++ {
        var version uint32
        if version, err = buf.GetUint32(); err != nil {
            return nil, err
        }
        manifest.Groups[ids[i]].Version = version
    }

    childIds := make([][]uint16, count)

    for i := 0; i < len(ids); i++ {
        var childCount uint16
        if childCount, err = buf.GetUint16(); err != nil {
            return nil, err
        }
        manifest.Groups[ids[i]].Files = make(map[uint16]*FileEntry, childCount)
        childIds[i] = make([]uint16, childCount)
    }

    for group := 0; group < len(ids); group++ {
        id := uint16(0)
        for j := 0; j < len(childIds[group]); j++ {
            skip, err := buf.GetUint16()
            if err != nil {
                return nil, err
            }
            id += skip

            manifest.Groups[ids[group]].Files[id] = new(FileEntry)
            childIds[group][j] = id
        }
    }

    if flags & NamedFlag != 0{
        for group := 0; group < len(ids); group++ {
            for child := 0; child < len(childIds[group]); child++ {
                var name uint32
                if name, err = buf.GetUint32(); err != nil {
                    return nil, err
                }
                manifest.Groups[ids[group]].Files[childIds[group][child]].Name = name
            }
        }
    }

    return manifest, nil
}
