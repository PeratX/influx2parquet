package tsm1

// ReadFloatBlockAt returns the float values corresponding to the given index entry.
func (t *TSMReader) ReadFloatBlockAt(entry *IndexEntry, vals *[]FloatValue) ([]FloatValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readFloatBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadIntegerBlockAt returns the integer values corresponding to the given index entry.
func (t *TSMReader) ReadIntegerBlockAt(entry *IndexEntry, vals *[]IntegerValue) ([]IntegerValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readIntegerBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadUnsignedBlockAt returns the unsigned values corresponding to the given index entry.
func (t *TSMReader) ReadUnsignedBlockAt(entry *IndexEntry, vals *[]UnsignedValue) ([]UnsignedValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readUnsignedBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadStringBlockAt returns the string values corresponding to the given index entry.
func (t *TSMReader) ReadStringBlockAt(entry *IndexEntry, vals *[]StringValue) ([]StringValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readStringBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// ReadBooleanBlockAt returns the boolean values corresponding to the given index entry.
func (t *TSMReader) ReadBooleanBlockAt(entry *IndexEntry, vals *[]BooleanValue) ([]BooleanValue, error) {
	t.mu.RLock()
	v, err := t.accessor.readBooleanBlock(entry, vals)
	t.mu.RUnlock()
	return v, err
}

// blockAccessor abstracts access to blocks in a TSM file.
type blockAccessor interface {
	init() (*indirectIndex, error)
	read(key []byte, timestamp int64) ([]Value, error)
	readAll(key []byte) ([]Value, error)
	readBlock(entry *IndexEntry, values []Value) ([]Value, error)
	readFloatBlock(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error)
	readIntegerBlock(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error)
	readUnsignedBlock(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error)
	readStringBlock(entry *IndexEntry, values *[]StringValue) ([]StringValue, error)
	readBooleanBlock(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error)
	readBytes(entry *IndexEntry, buf []byte) (uint32, []byte, error)
	rename(path string) error
	path() string
	close() error
	free() error
}

func (m *mmapAccessor) readFloatBlock(entry *IndexEntry, values *[]FloatValue) ([]FloatValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeFloatBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (m *mmapAccessor) readIntegerBlock(entry *IndexEntry, values *[]IntegerValue) ([]IntegerValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeIntegerBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (m *mmapAccessor) readUnsignedBlock(entry *IndexEntry, values *[]UnsignedValue) ([]UnsignedValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeUnsignedBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (m *mmapAccessor) readStringBlock(entry *IndexEntry, values *[]StringValue) ([]StringValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeStringBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (m *mmapAccessor) readBooleanBlock(entry *IndexEntry, values *[]BooleanValue) ([]BooleanValue, error) {
	m.incAccess()

	m.mu.RLock()
	if int64(len(m.b)) < entry.Offset+int64(entry.Size) {
		m.mu.RUnlock()
		return nil, ErrTSMClosed
	}

	a, err := DecodeBooleanBlock(m.b[entry.Offset+4:entry.Offset+int64(entry.Size)], values)
	m.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return a, nil
}
