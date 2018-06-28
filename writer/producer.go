package main

import (
	"math/rand"
)

type producer struct {
	count, keySize, valueSize int
	pipe                      chan *entry
	input                     []*entry
	value                     []byte
}

func newProducer(count, keySize, valueSize, parallel int) *producer {
	p := &producer{
		count:     count,
		keySize:   keySize,
		valueSize: valueSize,
		pipe:      make(chan *entry, parallel),
		input:     make([]*entry, count),
	}

	p.value = make([]byte, valueSize)
	rand.Read(p.value)
	return p
}

func (p *producer) output() <-chan *entry {
	return p.pipe
}

func (p *producer) len() int {
	return len(p.input)
}

func (p *producer) stop() {

}

func (p *producer) start() {
	defer close(p.pipe)
	for _, e := range p.input {
		p.pipe <- e
	}
}

func (p *producer) generateAll() {
	m := make(map[string]struct{})
	for i := 0; i < p.count; {
		e := p.generate(p.keySize, p.valueSize)
		if _, ok := m[string(e.key)]; !ok {
			p.input[i] = e
			m[string(e.key)] = struct{}{}
			i++
		}
		continue
	}
}

func (p *producer) generate(keySize, valueSize int) *entry {
	key := make([]byte, keySize)
	rand.Read(key)
	return &entry{key: key, value: p.value}
}
