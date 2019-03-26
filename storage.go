package coffee

import "sync"

type Storage struct {
    cache     *Cache
    archives  map[uint8]map[uint16][]byte
    mutex     sync.Mutex
}

func NewStorage(cache *Cache) (*Storage, error) {
    storage := Storage{
        cache:     cache,
        archives:  make(map[uint8]map[uint16][]byte),
    }

    storage.archives[ManifestPackage] = make(map[uint16][]byte)

    for _, id := range cache.PackageIds() {
        storage.archives[id] = make(map[uint16][]byte)
    }

    manifest, err := CreateReleaseManifest(cache)
    if err != nil {
        return nil, err
    }

    packed, _ := CompressArchive(Uncompressed, manifest)
    storage.archives[ManifestPackage][255] = packed

    return &storage, nil
}

func (s *Storage) GetArchive(index uint8, id uint16) ([]byte, error) {
    s.mutex.Lock()
    defer s.mutex.Unlock()

    if b, exists := s.archives[index][id]; exists {
        return b, nil
    }

    return s.retrieveArchive(index, id)
}

func (s *Storage) retrieveArchive(pkg uint8, id uint16) ([]byte, error) {
    b, err := s.cache.Get(pkg, id)
    if err != nil {
        return nil, err
    }

    // Trimming the archive fixes an issue with caches containing a footer which is used to hold to archive's version to
    // compare against the manifest.
    b, err = TrimArchive(b)
    if err != nil {
        return nil, err
    }

    s.archives[pkg][id] = b

    return b, nil
}
