package main

import "math/rand"

type producer struct {
	count, keySize, valueSize int
	pipe                      chan *entry
	input                     []*entry
	value                     []byte
	sameValue                 bool
}

func newProducer(count, keySize, valueSize, parallel int, sameValue bool) *producer {
	p := &producer{
		count:count,
		keySize:keySize,
		valueSize:valueSize,
		pipe: make(chan *entry, parallel),
		input:make([]*entry, count),
	}

	if sameValue {
		p.value = make([]byte, valueSize)
		rand.Read(p.value)
	}
	return p
}

func (p *producer) output() <-chan *entry  {
	return p.pipe
}

func (p *producer) len() int {
	return len(p.input)
}

func (p *producer) start() {
	defer close(p.pipe)
	for _, e := range p.input {
		p.pipe <- e
	}
}

func (p *producer) generateAll() {
	m  := make(map[string]struct{})
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
	if p.sameValue {
		return &entry{key: key, value: p.value}
	}
	value := make([]byte, valueSize)
	rand.Read(key)
	rand.Read(value)
	return &entry{key: key, value: value}
}
