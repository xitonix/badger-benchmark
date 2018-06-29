package common

import (
	"fmt"
	"os"
	"time"
)

type CSVWriter struct {
	file    *os.File
	headers []string
	exists  bool
}

func NewCSVWriter(filePath string) (*CSVWriter, error) {
	w := &CSVWriter{}
	if _, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			w.file, err = os.Create(filePath)
			if err != nil {
				return nil, err
			}
			return w, nil
		}
		return nil, err
	}

	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	w.file = f
	w.exists = true
	return w, nil

}

func (c *CSVWriter) AddHeaders(header ...string) {
	for _, h := range header {
		c.headers = append(c.headers, h)
	}
	c.headers = append(c.headers, "DURATION")
}

func (c *CSVWriter) Write(duration time.Duration, val ...interface{}) {
	if !c.exists {
		h := make([]interface{}, len(c.headers))
		for i, header := range c.headers {
			h[i] = header
		}
		c.write(h...)
		c.file.WriteString("\n")
	}
	c.write(val...)
	c.file.WriteString(fmt.Sprintf(",\"%v\"", duration))
	c.file.WriteString("\n")
}

func (c *CSVWriter) write(val ...interface{}) {
	for i, v := range val {
		c.file.WriteString(fmt.Sprintf("\"%v\"", v))
		if i < len(val)-1 {
			c.file.WriteString(fmt.Sprintf(","))
		}
	}
}
