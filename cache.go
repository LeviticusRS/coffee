package coffee

import (
    "fmt"
    "io"
    "io/ioutil"
    "os"
    "path"
    "path/filepath"
    "strconv"
    "strings"
    "sync"
)

const (
    referenceLength    = 6
    blockHeaderLength  = 8
    blockPayloadLength = 512
    blockLength        = blockHeaderLength + blockPayloadLength
    endOfArchive       = 0
)

type Cache struct {
    blocks         *os.File
    manifestIndex  *os.File
    packageIds     []uint8
    packageIndexes []*os.File
    mutex          sync.Mutex
    buffer         [blockLength]byte
}

func OpenCache(root string) (*Cache, error) {
    blocks, err := os.Open(path.Join(root, "main_file_cache.dat2"))
    if err != nil {
        return nil, err
    }

    manifestIndex, err := os.Open(path.Join(root, fmt.Sprintf("main_file_cache.idx%d", ManifestPackage)))
    if err != nil {
        return nil, err
    }

    files, err := ioutil.ReadDir(root)
    if err != nil {
        return nil, err
    }

    packageIds := make([]uint8, 0)
    maximumId := uint8(0)
    for _, file := range files {
        if file.IsDir() || !strings.Contains(file.Name(), ".idx") {
            continue
        }

        suffix := file.Name()[strings.Index(file.Name(), ".idx")+4:]

        n, err := strconv.ParseInt(suffix, 10, 16)
        if err != nil {
            return nil, err
        }

        id := uint8(n)

        if n == ManifestPackage {
            continue
        }

        packageIds = append(packageIds, id)

        if id > maximumId {
            maximumId = id + 1
        }
    }

    indexes := make([]*os.File, maximumId+1)
    for _, id := range packageIds {
        index, err := os.Open(filepath.Join(root, fmt.Sprintf("main_file_cache.idx%d", id)))
        if err != nil {
            return nil, err
        }
        indexes[id] = index
    }

    return &Cache{
        blocks:         blocks,
        manifestIndex:  manifestIndex,
        packageIds:     packageIds,
        packageIndexes: indexes,
        mutex:          sync.Mutex{},
    }, nil
}

func (c *Cache) PackageIds() []uint8 {
    return c.packageIds
}

func (c *Cache) Get(pkg uint8, id uint16) ([]byte, error) {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    index, err := c.getPackageIndex(pkg)
    if err != nil {
        return nil, err
    }

    indexLength, err := fileLength(index)
    if err != nil {
        return nil, err
    }

    if indexLength < referenceLength+referenceLength*int64(id) {
        return nil, fmt.Errorf("coffee: archive does not exist (package: %d, id: %d)", pkg, id)
    }

    if _, err := index.ReadAt(c.buffer[:referenceLength], int64(id)*referenceLength); err != nil {
        return nil, err
    }

    length := uint32(c.buffer[0])<<16 | uint32(c.buffer[1])<<8 | uint32(c.buffer[2])
    block := uint32(c.buffer[3])<<16 | uint32(c.buffer[4])<<8 | uint32(c.buffer[5])

    blocksLength, err := fileLength(c.blocks)
    if err != nil {
        return nil, err
    }

    if block <= endOfArchive || int64(block) > blocksLength/int64(blockLength) {
        return nil, io.EOF
    }

    b := make([]byte, length)

    offset := uint32(0)
    part := uint16(0)

    for offset < length {
        if block == endOfArchive {
            return nil, fmt.Errorf("coffee: premature end of archive (package: %d, archive: %d)", pkg, id)
        }

        read := length - offset
        if read > blockPayloadLength {
            read = blockPayloadLength
        }

        if _, err := c.blocks.ReadAt(c.buffer[:blockHeaderLength+read], int64(block)*blockLength); err != nil {
            return nil, err
        }

        blockArchiveId := uint16(c.buffer[0])<<8 | uint16(c.buffer[1])
        blockArchiveChunk := uint16(c.buffer[2])<<8 | uint16(c.buffer[3])
        blockPackage := c.buffer[7]

        if blockArchiveId != id || blockArchiveChunk != part || blockPackage != pkg {
            return nil, fmt.Errorf("coffee: invalid block header (package: %d, archive: %d)", pkg, id)
        }

        nextBlock := uint32(c.buffer[4])<<16 | uint32(c.buffer[5])<<8 | uint32(c.buffer[6])

        if int64(block) > blocksLength/int64(blockLength) {
            return nil, io.EOF
        }

        copy(b[offset:], c.buffer[blockHeaderLength:blockHeaderLength+read])

        block = nextBlock
        offset += read
        part++
    }

    return b, nil
}

func (c *Cache) getPackageIndex(pkg uint8) (*os.File, error) {
    if pkg == ManifestPackage {
        return c.manifestIndex, nil
    }

    if len(c.packageIndexes) < int(pkg) {
        return nil, fmt.Errorf("coffee: cache does not contain package - %d", pkg)
    }

    return c.packageIndexes[pkg], nil
}

func fileLength(file *os.File) (int64, error) {
    info, err := file.Stat()
    if err != nil {
        return 0, err
    }
    return info.Size(), nil
}