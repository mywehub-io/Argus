package emitter

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/wehubfusion/Argus/pkg/event"
)

type captureObserver struct{ events []*event.Event }

func (c *captureObserver) Emit(_ context.Context, e *event.Event) error {
	c.events = append(c.events, e)
	return nil
}
func (c *captureObserver) Close(context.Context) error { return nil }

type fakeWriter struct {
	err     error
	targets []DocumentTarget
	data    [][]byte
}

func (f *fakeWriter) WriteDocument(_ context.Context, t DocumentTarget, data []byte) (event.FileRef, error) {
	if f.err != nil {
		return event.FileRef{}, f.err
	}
	f.targets = append(f.targets, t)
	f.data = append(f.data, data)
	return event.FileRef{Path: "results/w/r/" + t.NodeID + "/" + t.Direction + ".json", Size: int64(len(data)), ContentType: "application/json"}, nil
}

const fileOut = `{"rows":2,"encoded":{"$file":{"path":"results/w/r/n/encoded.csv","size":10,"contentType":"text/csv"}},` +
	`"files":[{"$file":{"path":"results/w/r/n/a.txt","size":3},"key":"a"}]}`

func TestNodeEndSendsTheDocument(t *testing.T) {
	obs := &captureObserver{}
	w := &fakeWriter{}
	em := NewArgusNodeEndEmitter(obs, nil, nil).WithDocuments(w)
	var out interface{}
	_ = json.Unmarshal([]byte(fileOut), &out)
	if err := em.EmitNodeEnd(context.Background(), NodeEndEmitParams{ClientID: "c", WorkflowID: "w", RunID: "r", NodeID: "n", Output: out}); err != nil {
		t.Fatal(err)
	}
	var end event.EndNode
	if err := json.Unmarshal(obs.events[0].Data, &end); err != nil {
		t.Fatal(err)
	}
	p := end.Output
	if p == nil || p.Document == nil || p.Document.Path != "results/w/r/n/output.json" || len(p.InlineData) != 0 || p.BlobReference != nil {
		t.Fatalf("want only a document reference: %+v", p)
	}
	if len(p.Files) != 2 {
		t.Fatalf("want 2 files, got %+v", p.Files)
	}
	keys := map[string]string{}
	for _, f := range p.Files {
		keys[f.Key] = f.File.Path
		if f.Direction != DirectionOutput {
			t.Fatalf("direction %q", f.Direction)
		}
	}
	if keys["/encoded"] != "results/w/r/n/encoded.csv" || keys["/files/0"] != "results/w/r/n/a.txt" {
		t.Fatalf("file keys wrong: %v", keys)
	}
}

func TestNodeStartSendsTheDocument(t *testing.T) {
	obs := &captureObserver{}
	w := &fakeWriter{}
	em := NewArgusNodeStartEmitter(obs, nil, nil).WithDocuments(w)
	in := []byte(`{"data":{"$file":{"path":"results/w/r/trigger/payload.csv","size":5}}}`)
	if err := em.EmitNodeStart(context.Background(), NodeStartEmitParams{ClientID: "c", WorkflowID: "w", RunID: "r", NodeID: "n", Input: in}); err != nil {
		t.Fatal(err)
	}
	var start event.StartNode
	_ = json.Unmarshal(obs.events[0].Data, &start)
	if start.Input == nil || start.Input.Document == nil || w.targets[0].Direction != DirectionInput || string(w.data[0]) != string(in) {
		t.Fatalf("input document not written and sent: %+v", start.Input)
	}
	if len(start.Input.Files) != 1 || start.Input.Files[0].Key != "/data" {
		t.Fatalf("input files: %+v", start.Input.Files)
	}
}

// A failed write falls back to the inline payload, still listing the files.
func TestDocumentWriteFailureFallsBack(t *testing.T) {
	obs := &captureObserver{}
	em := NewArgusNodeStartEmitter(obs, nil, nil).WithDocuments(&fakeWriter{err: errors.New("store down")})
	in := []byte(`{"data":{"$file":{"path":"results/w/r/trigger/payload.csv","size":5}}}`)
	if err := em.EmitNodeStart(context.Background(), NodeStartEmitParams{ClientID: "c", WorkflowID: "w", RunID: "r", NodeID: "n", Input: in}); err != nil {
		t.Fatal(err)
	}
	var start event.StartNode
	_ = json.Unmarshal(obs.events[0].Data, &start)
	if start.Input == nil || start.Input.Document != nil || len(start.Input.InlineData) == 0 || len(start.Input.Files) != 1 {
		t.Fatalf("want the inline fallback with files: %+v", start.Input)
	}
}

func TestScanFilesIgnoresDataThatIsNotARef(t *testing.T) {
	if got := ScanFiles([]byte(`{"$file":"not an object","x":{"$file":{"size":3}}}`), DirectionInput); len(got) != 0 {
		t.Fatalf("want none, got %+v", got)
	}
	if got := ScanFiles([]byte(`not json`), DirectionInput); got != nil {
		t.Fatal("non-JSON has no files")
	}
}
