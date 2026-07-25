package proto

import "encoding/json"

// JSON is the text wire format: "VERB {json}". It is the fallback every
// client can speak, and the one you can type by hand into a console.
type JSON struct{}

func (JSON) Name() string { return NameJSON }

func (JSON) Binary() bool { return false }

func (JSON) Encode(verb string, payload any) ([]byte, error) { return Format(verb, payload) }

func (JSON) Decode(frame []byte) (string, []byte, error) { return Split(frame) }

func (JSON) Unmarshal(payload []byte, v any) error { return json.Unmarshal(payload, v) }
