package remotewrite

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/textproto"
	"sort"
)

const durableEnvelopeVersion = 1
const durableRecordRefVersion = 1

var durableEnvelopeMagic = [4]byte{'R', 'W', 'S', 'Q'}
var durableRecordRefMagic = [4]byte{'R', 'W', 'R', 'F'}

type headerEntry struct {
	key    string
	values []string
}

func encodeDurableRequestEnvelope(req DurableRequest) ([]byte, error) {
	headers := canonicalHeaderEntries(req.ExtraHeaders)

	size := len(durableEnvelopeMagic) + 1
	size += encodedBytesLen(req.TenantID)
	size += encodedBytesLen(req.ContentType)
	size += encodedBytesLen(req.ContentEncoding)
	size += encodedBytesLen(req.RemoteWriteVersion)
	size += encodedUvarintLen(uint64(len(headers)))
	for _, entry := range headers {
		size += encodedBytesLen(entry.key)
		size += encodedUvarintLen(uint64(len(entry.values)))
		for _, value := range entry.values {
			size += encodedBytesLen(value)
		}
	}
	size += encodedBlockLen(req.BodyBytes)

	buf := make([]byte, 0, size)
	buf = append(buf, durableEnvelopeMagic[:]...)
	buf = append(buf, durableEnvelopeVersion)
	buf = appendStringField(buf, req.TenantID)
	buf = appendStringField(buf, req.ContentType)
	buf = appendStringField(buf, req.ContentEncoding)
	buf = appendStringField(buf, req.RemoteWriteVersion)
	buf = appendUvarintField(buf, uint64(len(headers)))
	for _, entry := range headers {
		buf = appendStringField(buf, entry.key)
		buf = appendUvarintField(buf, uint64(len(entry.values)))
		for _, value := range entry.values {
			buf = appendStringField(buf, value)
		}
	}
	buf = appendBytesField(buf, req.BodyBytes)
	
	return buf, nil
}

// decodeDurableRequestEnvelope parses data in place; BodyBytes aliases data.
func decodeDurableRequestEnvelope(data []byte) (DurableRequest, error) {
	if len(data) < len(durableEnvelopeMagic)+1 {
		return DurableRequest{}, fmt.Errorf("durable envelope too short")
	}
	if string(data[:len(durableEnvelopeMagic)]) != string(durableEnvelopeMagic[:]) {
		return DurableRequest{}, fmt.Errorf("durable envelope has unknown magic")
	}

	idx := len(durableEnvelopeMagic)
	version := data[idx]
	idx++
	if version != durableEnvelopeVersion {
		return DurableRequest{}, fmt.Errorf("durable envelope version %d is unsupported", version)
	}

	tenantID, err := readStringField(data, &idx)
	if err != nil {
		return DurableRequest{}, fmt.Errorf("read tenant id: %w", err)
	}
	contentType, err := readStringField(data, &idx)
	if err != nil {
		return DurableRequest{}, fmt.Errorf("read content type: %w", err)
	}
	contentEncoding, err := readStringField(data, &idx)
	if err != nil {
		return DurableRequest{}, fmt.Errorf("read content encoding: %w", err)
	}
	remoteWriteVersion, err := readStringField(data, &idx)
	if err != nil {
		return DurableRequest{}, fmt.Errorf("read remote write version: %w", err)
	}
	headerCount, err := readUvarintField(data, &idx)
	if err != nil {
		return DurableRequest{}, fmt.Errorf("read header count: %w", err)
	}

	var headers http.Header
	if headerCount > 0 {
		headers = make(http.Header, int(headerCount))
		for i := uint64(0); i < headerCount; i++ {
			key, err := readStringField(data, &idx)
			if err != nil {
				return DurableRequest{}, fmt.Errorf("read header key %d: %w", i, err)
			}
			valueCount, err := readUvarintField(data, &idx)
			if err != nil {
				return DurableRequest{}, fmt.Errorf("read header value count %q: %w", key, err)
			}
			values := make([]string, 0, valueCount)
			for j := uint64(0); j < valueCount; j++ {
				value, err := readStringField(data, &idx)
				if err != nil {
					return DurableRequest{}, fmt.Errorf("read header value %q[%d]: %w", key, j, err)
				}
				values = append(values, value)
			}
			headers[key] = values
		}
	}

	bodyBytes, err := readBytesField(data, &idx)
	if err != nil {
		return DurableRequest{}, fmt.Errorf("read body: %w", err)
	}
	if idx != len(data) {
		return DurableRequest{}, fmt.Errorf("durable envelope has %d trailing bytes", len(data)-idx)
	}

	return DurableRequest{
		TenantID:           tenantID,
		BodyBytes:          bodyBytes,
		ContentType:        contentType,
		ContentEncoding:    contentEncoding,
		RemoteWriteVersion: remoteWriteVersion,
		ExtraHeaders:       headers,
	}, nil
}

func canonicalHeaderEntries(headers http.Header) []headerEntry {
	if len(headers) == 0 {
		return nil
	}
	rawKeys := make([]string, 0, len(headers))
	for key, values := range headers {
		if len(values) > 0 {
			rawKeys = append(rawKeys, key)
		}
	}
	sort.Slice(rawKeys, func(i, j int) bool {
		left := textproto.CanonicalMIMEHeaderKey(rawKeys[i])
		right := textproto.CanonicalMIMEHeaderKey(rawKeys[j])
		if left == right {
			return rawKeys[i] < rawKeys[j]
		}
		return left < right
	})

	entries := make([]headerEntry, 0, len(rawKeys))
	for _, rawKey := range rawKeys {
		values := headers[rawKey]
		if len(values) == 0 {
			continue
		}
		key := textproto.CanonicalMIMEHeaderKey(rawKey)
		if len(entries) > 0 && entries[len(entries)-1].key == key {
			entries[len(entries)-1].values = append(entries[len(entries)-1].values, values...)
			continue
		}
		copied := make([]string, len(values))
		copy(copied, values)
		entries = append(entries, headerEntry{key: key, values: copied})
	}

	return entries
}

func appendUvarintField(dst []byte, n uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(buf[:], n)

	return append(dst, buf[:size]...)
}

func appendStringField(dst []byte, value string) []byte {
	dst = appendUvarintField(dst, uint64(len(value)))

	return append(dst, value...)
}

func appendBytesField(dst, value []byte) []byte {
	dst = appendUvarintField(dst, uint64(len(value)))

	return append(dst, value...)
}

func readUvarintField(data []byte, idx *int) (uint64, error) {
	if *idx >= len(data) {
		return 0, fmt.Errorf("unexpected EOF")
	}
	value, n := binary.Uvarint(data[*idx:])
	if n <= 0 {
		return 0, fmt.Errorf("invalid uvarint")
	}
	*idx += n

	return value, nil
}

func readStringField(data []byte, idx *int) (string, error) {
	buf, err := readBytesField(data, idx)
	if err != nil {
		return "", err
	}

	return string(buf), nil
}

func readBytesField(data []byte, idx *int) ([]byte, error) {
	size, err := readUvarintField(data, idx)
	if err != nil {
		return nil, err
	}
	if size > uint64(len(data)-*idx) {
		return nil, fmt.Errorf("unexpected EOF")
	}
	start := *idx
	*idx += int(size)

	return data[start:*idx], nil
}

func encodedBytesLen(value string) int {
	return encodedBlockLen([]byte(value))
}

func encodedBlockLen(value []byte) int {
	return encodedUvarintLen(uint64(len(value))) + len(value)
}

func encodedUvarintLen(value uint64) int {
	var buf [binary.MaxVarintLen64]byte

	return binary.PutUvarint(buf[:], value)
}

func encodeDurableRecordRef(recordID uint64) []byte {
	size := len(durableRecordRefMagic) + 1 + encodedUvarintLen(recordID)
	buf := make([]byte, 0, size)
	buf = append(buf, durableRecordRefMagic[:]...)
	buf = append(buf, durableRecordRefVersion)

	return appendUvarintField(buf, recordID)
}

func decodeDurableRecordRef(data []byte) (uint64, error) {
	if len(data) < len(durableRecordRefMagic)+1 {
		return 0, fmt.Errorf("durable record ref too short")
	}
	if string(data[:len(durableRecordRefMagic)]) != string(durableRecordRefMagic[:]) {
		return 0, fmt.Errorf("durable record ref has unknown magic")
	}

	idx := len(durableRecordRefMagic)
	version := data[idx]
	idx++
	if version != durableRecordRefVersion {
		return 0, fmt.Errorf("durable record ref version %d is unsupported", version)
	}

	recordID, err := readUvarintField(data, &idx)
	if err != nil {
		return 0, err
	}
	if idx != len(data) {
		return 0, fmt.Errorf("durable record ref has %d trailing bytes", len(data)-idx)
	}

	return recordID, nil
}
