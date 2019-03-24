package coffee

import (
    "errors"
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
    ManifestIndex = 255
)

const (
    referenceLength    = 6
    blockHeaderLength  = 8
    blockPayloadLength = 512
    blockLength        = blockHeaderLength + blockPayloadLength
    endOfArchive       = 0
)

type Cache struct {
    blocks        *os.File
    manifestIndex *os.File
    indexes       []*os.File
    mutex         sync.Mutex
    buffer        [blockLength]byte
}

func OpenCache(root string) (*Cache, error) {
    blocks, err := os.Open(path.Join(root, "main_file_cache.dat2"))
    if err != nil {
        return nil, err
    }

    manifestIndex, err := os.Open(path.Join(root, fmt.Sprintf("main_file_cache.idx%d", ManifestIndex)))
    if err != nil {
        return nil, err
    }

    files, err := ioutil.ReadDir(root)
    if err != nil {
        return nil, err
    }

    count := 0
    for _, file := range files {
        if file.IsDir() {
            continue
        }

        if !strings.Contains(file.Name(), ".idx") {
            continue
        }

        id, err := strconv.Atoi(file.Name()[strings.Index(file.Name(), ".idx")+4:])
        if err != nil {
            return nil, err
        }

        if id == ManifestIndex {
            continue
        }

        if id > count {
            count = id + 1
        }
    }

    indexes := make([]*os.File, count)
    for i := 0; i < count; i++ {
        index, err := os.Open(filepath.Join(root, fmt.Sprintf("main_file_cache.idx%d", i)))
        if err != nil {
            return nil, err
        }
        indexes[i] = index
    }

    return &Cache{
        blocks:        blocks,
        manifestIndex: manifestIndex,
        indexes:       indexes,
        mutex:         sync.Mutex{},
    }, nil
}

func (c *Cache) Length() int {
    return len(c.indexes)
}

func (c *Cache) Get(index, id int) ([]byte, error) {
    c.mutex.Lock()
    defer c.mutex.Unlock()

    indexFile, err := c.getIndexFile(index)
    if err != nil {
        return nil, err
    }

    indexLength, err := fileLength(indexFile)
    if err != nil {
        return nil, err
    }

    if indexLength < referenceLength+referenceLength*int64(id) {
        return nil, errors.New("asset: archive does not exist")
    }

    if _, err := indexFile.ReadAt(c.buffer[:referenceLength], int64(id)*referenceLength); err != nil {
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

    result := make([]byte, length)

    offset := uint32(0)
    part := uint16(0)

    for offset < length {
        if block == endOfArchive {
            return nil, errors.New("asset: premature end of archive")
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
        blockIndex := c.buffer[7]

        if blockArchiveId != uint16(id) || blockArchiveChunk != part || blockIndex != uint8(index) {
            return nil, errors.New("asset: invalid block header")
        }

        nextBlock := uint32(c.buffer[4])<<16 | uint32(c.buffer[5])<<8 | uint32(c.buffer[6])

        if int64(block) > blocksLength/int64(blockLength) {
            return nil, io.EOF
        }

        copy(result[offset:], c.buffer[blockHeaderLength:blockHeaderLength+read])

        block = nextBlock
        offset += read
        part++
    }

    return result, nil
}


func (c *Cache) getIndexFile(index int) (*os.File, error) {
    if index == ManifestIndex {
        return c.manifestIndex, nil
    }

    if len(c.indexes) < int(index) {
        return nil, fmt.Errorf("asset: cache does not contain index %d", index)
    }

    return c.indexes[index], nil
}

func fileLength(file *os.File) (int64, error) {
    info, err := file.Stat()
    if err != nil {
        return 0, err
    }
    return info.Size(), nil
}
