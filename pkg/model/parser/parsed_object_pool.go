package parser

import (
	"context"
	"sync"

	"github.com/buildbarn/bb-storage/pkg/eviction"
	model_core "github.com/buildbarn/bonanza/pkg/model/core"
	"github.com/buildbarn/bonanza/pkg/storage/object"
)

type ParsedObjectEvictionKey struct {
	// TODO: Have a stable key for identifying readers. That will
	// allow us to get cache hits between builds.
	reader    any
	reference object.LocalReference
}

type cachedParsedObject struct {
	parsedObject any
	sizeBytes    int
}

type ParsedObjectPool struct {
	lock               sync.Mutex
	objects            map[ParsedObjectEvictionKey]cachedParsedObject
	evictionSet        eviction.Set[ParsedObjectEvictionKey]
	remainingCount     int
	remainingSizeBytes int
}

func NewParsedObjectPool(evictionSet eviction.Set[ParsedObjectEvictionKey], maximumCount, maximumSizeBytes int) *ParsedObjectPool {
	return &ParsedObjectPool{
		objects:            map[ParsedObjectEvictionKey]cachedParsedObject{},
		evictionSet:        evictionSet,
		remainingCount:     maximumCount,
		remainingSizeBytes: maximumSizeBytes,
	}
}

type ParsedObjectPoolIngester[TReference any] struct {
	pool      *ParsedObjectPool
	rawReader ParsedObjectReader[TReference, model_core.Message[[]byte, TReference]]
}

func NewParsedObjectPoolIngester[TReference any](
	pool *ParsedObjectPool,
	rawReader ParsedObjectReader[TReference, model_core.Message[[]byte, TReference]],
) *ParsedObjectPoolIngester[TReference] {
	return &ParsedObjectPoolIngester[TReference]{
		pool:      pool,
		rawReader: rawReader,
	}
}

type poolBackedParsedObjectReader[TReference object.BasicReference, TParsedObject any] struct {
	ingester *ParsedObjectPoolIngester[TReference]
	parser   ObjectParser[TReference, TParsedObject]
}

func LookupParsedObjectReader[TReference object.BasicReference, TParsedObject any](
	ingester *ParsedObjectPoolIngester[TReference],
	parser ObjectParser[TReference, TParsedObject],
) ParsedObjectReader[TReference, TParsedObject] {
	return &poolBackedParsedObjectReader[TReference, TParsedObject]{
		ingester: ingester,
		parser:   parser,
	}
}

func (r *poolBackedParsedObjectReader[TReference, TParsedObject]) ReadParsedObject(ctx context.Context, reference TReference) (TParsedObject, error) {
	insertionKey := ParsedObjectEvictionKey{
		reader:    r,
		reference: reference.GetLocalReference(),
	}

	i := r.ingester
	p := i.pool
	p.lock.Lock()
	if object, ok := p.objects[insertionKey]; ok {
		// Return cached instance of the parsed object.
		p.evictionSet.Touch(insertionKey)
		p.lock.Unlock()
		return object.parsedObject.(TParsedObject), nil
	}
	p.lock.Unlock()

	raw, err := i.rawReader.ReadParsedObject(ctx, reference)
	if err != nil {
		var badParsedObject TParsedObject
		return badParsedObject, err
	}

	parsedObject, parsedObjectSizeBytes, err := r.parser.ParseObject(raw)
	if err != nil {
		var badParsedObject TParsedObject
		return badParsedObject, err
	}
	sizeBytes := reference.GetSizeBytes() - len(raw.Message) + parsedObjectSizeBytes

	p.lock.Lock()
	if _, ok := p.objects[insertionKey]; ok {
		// Race: parsed object was inserted into the cache by
		// another goroutine while we were parsing it as well.
		p.evictionSet.Touch(insertionKey)
	} else {
		p.objects[insertionKey] = cachedParsedObject{
			parsedObject: parsedObject,
			sizeBytes:    sizeBytes,
		}
		p.remainingCount--
		p.remainingSizeBytes -= sizeBytes
		p.evictionSet.Insert(insertionKey)

		// Evict objects if we're consuming too much space.
		for p.remainingCount < 0 || p.remainingSizeBytes < 0 {
			removalKey := p.evictionSet.Peek()
			removedSizeBytes := p.objects[removalKey].sizeBytes
			delete(p.objects, removalKey)

			p.remainingCount++
			p.remainingSizeBytes += removedSizeBytes
			p.evictionSet.Remove()
		}
	}
	p.lock.Unlock()

	return parsedObject, nil
}
