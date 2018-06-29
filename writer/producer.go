package main

import (
	"math/rand"
	"strconv"
	"sync"
)

type producer struct {
	count, keySize, valueSize int
	pipe                      chan *entry
	value                     []byte
	key                       []byte
	wg                        sync.WaitGroup
	cancelled                 bool
}

func newProducer(count, keySize, valueSize, parallel int) *producer {
	p := &producer{
		count:     count,
		keySize:   keySize,
		valueSize: valueSize,
		pipe:      make(chan *entry, parallel),
	}

	p.value = make([]byte, valueSize)
	p.key = make([]byte, keySize)
	rand.Read(p.value)
	rand.Read(p.key)
	return p
}

func (p *producer) output() <-chan *entry {
	return p.pipe
}

func (p *producer) stop() {
	p.cancelled = true
	p.wg.Wait()
}

func (p *producer) start() {
	p.wg.Add(1)
	go func() {
		defer close(p.pipe)
		for i := 0; i < p.count; i++ {
			if p.cancelled {
				break
			}

			k := make([]byte, p.keySize)
			copy(k, []byte(strconv.Itoa(i)))
			p.pipe <- &entry{key: k, value: p.value}
		}
	}()
}
