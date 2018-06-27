package main

import "math/rand"

type producer struct {
	count, keySize, valueSize int
	pipe chan *entry
}

func newProducer(count, keySize, valueSize, parallel int) *producer {
	return &producer{
		count:count,
		keySize:keySize,
		valueSize:valueSize,
		pipe: make(chan *entry, parallel),
	}
}

func (p *producer) output() <-chan *entry  {
	return p.pipe
}


func (p *producer) start() {
	defer close(p.pipe)
	for i := 0; i < p.count; i++ {
		p.pipe <- generate(p.keySize, p.valueSize)
	}
}

func generate(keySize, valueSize int) *entry {
	key := make([]byte, keySize)
	value := make([]byte, valueSize)
	rand.Read(key)
	rand.Read(value)
	return &entry{key: key, value: value}
}
