package emitter

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/wehubfusion/Argus/pkg/event"
)

// Directions of a node document and its files.
const (
	DirectionInput  = "input"
	DirectionOutput = "output"
)

// DocumentTarget names the node document to write.
type DocumentTarget struct {
	WorkflowID string
	RunID      string
	NodeID     string
	// Direction is DirectionInput or DirectionOutput.
	Direction string
}

// DocumentWriter writes a node's input or output document into the run's own files and returns
// its reference (raw payloads D9). With one set, the lifecycle emitters send that reference
// instead of a copy of the payload.
type DocumentWriter interface {
	WriteDocument(ctx context.Context, target DocumentTarget, data []byte) (event.FileRef, error)
}

// maxFileScanDepth bounds the walk over a document looking for file references.
const maxFileScanDepth = 16

// ScanFiles returns every file reference ({"$file":{...}}, alone or as a files list item) in a
// JSON document, keyed by its JSON pointer. A document that is not JSON has none.
func ScanFiles(data []byte, direction string) []event.PortFile {
	if len(data) == 0 {
		return nil
	}
	var doc interface{}
	if json.Unmarshal(data, &doc) != nil {
		return nil
	}
	var out []event.PortFile
	scanFiles(doc, "", direction, 0, &out)
	return out
}

func scanFiles(v interface{}, ptr, direction string, depth int, out *[]event.PortFile) {
	if depth > maxFileScanDepth {
		return
	}
	switch t := v.(type) {
	case map[string]interface{}:
		if ref, ok := parseFileRef(t["$file"]); ok {
			key := ptr
			if key == "" {
				key = "/"
			}
			*out = append(*out, event.PortFile{Key: key, Direction: direction, File: ref})
			return
		}
		for k, x := range t {
			scanFiles(x, ptr+"/"+escapePointer(k), direction, depth+1, out)
		}
	case []interface{}:
		for i, x := range t {
			scanFiles(x, ptr+"/"+strconv.Itoa(i), direction, depth+1, out)
		}
	}
}

// parseFileRef reads the object under "$file". A reference needs a path; the rest is optional.
func parseFileRef(v interface{}) (event.FileRef, bool) {
	m, ok := v.(map[string]interface{})
	if !ok {
		return event.FileRef{}, false
	}
	path, _ := m["path"].(string)
	if strings.TrimSpace(path) == "" {
		return event.FileRef{}, false
	}
	ref := event.FileRef{Path: path}
	if n, ok := m["size"].(float64); ok {
		ref.Size = int64(n)
	}
	ref.ContentType, _ = m["contentType"].(string)
	ref.FileName, _ = m["fileName"].(string)
	if n, ok := m["records"].(float64); ok {
		r := int64(n)
		ref.Records = &r
	}
	return ref, true
}

func escapePointer(k string) string {
	return strings.ReplaceAll(strings.ReplaceAll(k, "~", "~0"), "/", "~1")
}

// documentPayload writes data as the node's document and returns the payload that points at it,
// with the files found in it.
func documentPayload(ctx context.Context, w DocumentWriter, target DocumentTarget, data []byte) (*event.Payload, error) {
	ref, err := w.WriteDocument(ctx, target, data)
	if err != nil {
		return nil, err
	}
	return &event.Payload{Document: &ref, Files: ScanFiles(data, target.Direction)}, nil
}
