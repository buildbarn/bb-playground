package parser

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/eviction"
)

type CachedParsedObjectEvictionKey[TReference any] struct {
	deleterIndex int
	reference    TReference
}

type cachedParsedObjectDeleter[TReference any] interface {
	deleteCachedParsedObject(reference TReference) int
}

type CachedParsedObjectPool[TReference any] struct {
	lock                                sync.Mutex
	evictionSet                         eviction.Set[CachedParsedObjectEvictionKey[TReference]]
	remainingParsedObjectCount          int
	remainingParsedObjectTotalSizeBytes int
	deleters                            []cachedParsedObjectDeleter[TReference]
}

func NewCachedParsedObjectPool[TReference any](evictionSet eviction.Set[CachedParsedObjectEvictionKey[TReference]], parsedObjectCount, parsedObjectTotalSizeBytes int) *CachedParsedObjectPool[TReference] {
	return &CachedParsedObjectPool[TReference]{
		evictionSet:                         evictionSet,
		remainingParsedObjectCount:          parsedObjectCount,
		remainingParsedObjectTotalSizeBytes: parsedObjectTotalSizeBytes,
	}
}

type cachedParsedObject[TParsedObject any] struct {
	parsedObject TParsedObject
	sizeBytes    int
}

type cachingParsedObjectReader[TReference comparable, TParsedObject any] struct {
	base         ParsedObjectReader[TReference, TParsedObject]
	pool         *CachedParsedObjectPool[TReference]
	deleterIndex int

	cachedParsedObjects map[TReference]cachedParsedObject[TParsedObject]
}

func NewCachingParsedObjectReader[TReference comparable, TParsedObject any](base ParsedObjectReader[TReference, TParsedObject], pool *CachedParsedObjectPool[TReference]) ParsedObjectReader[TReference, TParsedObject] {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	r := &cachingParsedObjectReader[TReference, TParsedObject]{
		base:         base,
		pool:         pool,
		deleterIndex: len(pool.deleters),

		cachedParsedObjects: map[TReference]cachedParsedObject[TParsedObject]{},
	}
	pool.deleters = append(pool.deleters, r)
	return r
}

func (r *cachingParsedObjectReader[TReference, TParsedObject]) deleteCachedParsedObject(reference TReference) int {
	sizeBytes := r.cachedParsedObjects[reference].sizeBytes
	delete(r.cachedParsedObjects, reference)
	return sizeBytes
}

func (r *cachingParsedObjectReader[TReference, TParsedObject]) ReadParsedObject(ctx context.Context, reference TReference) (TParsedObject, int, error) {
	insertionKey := CachedParsedObjectEvictionKey[TReference]{
		deleterIndex: r.deleterIndex,
		reference:    reference,
	}
	p := r.pool
	p.lock.Lock()
	if cachedParsedObject, ok := r.cachedParsedObjects[reference]; ok {
		// Return cached instance of the parsed object.
		p.evictionSet.Touch(insertionKey)
		p.lock.Unlock()
		return cachedParsedObject.parsedObject, cachedParsedObject.sizeBytes, nil
	}
	p.lock.Unlock()

	parsedObject, sizeBytes, err := r.base.ReadParsedObject(ctx, reference)
	if err != nil {
		var badParsedObject TParsedObject
		return badParsedObject, 0, err
	}

	p.lock.Lock()
	if _, ok := r.cachedParsedObjects[reference]; ok {
		// Race: parsed object was inserted into the cache by
		// another goroutine while we were parsing it as well.
		p.evictionSet.Touch(insertionKey)
	} else {
		r.cachedParsedObjects[reference] = cachedParsedObject[TParsedObject]{
			parsedObject: parsedObject,
			sizeBytes:    sizeBytes,
		}
		p.remainingParsedObjectCount--
		p.remainingParsedObjectTotalSizeBytes -= sizeBytes
		p.evictionSet.Insert(insertionKey)

		// Evict objects if we're consuming too much space.
		for p.remainingParsedObjectCount < 0 || p.remainingParsedObjectTotalSizeBytes < 0 {
			removalKey := p.evictionSet.Peek()
			p.remainingParsedObjectCount++
			p.remainingParsedObjectTotalSizeBytes += p.deleters[removalKey.deleterIndex].deleteCachedParsedObject(removalKey.reference)
			p.evictionSet.Remove()
		}
	}
	p.lock.Unlock()

	return parsedObject, sizeBytes, nil
}
