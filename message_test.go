package jsonrpc_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/paularlott/jsonrpc"
)

func TestResponseMarshalSuccess(t *testing.T) {
	r := jsonrpc.Response{Result: json.RawMessage(`42`), ID: json.RawMessage(`7`)}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, `"result":42`) || !strings.Contains(got, `"id":7`) || !strings.Contains(got, `"jsonrpc":"2.0"`) {
		t.Errorf("marshal = %s", got)
	}
	if strings.Contains(got, `"error"`) {
		t.Errorf("success response contains error: %s", got)
	}
}

func TestResponseMarshalErrorHasNoResult(t *testing.T) {
	r := jsonrpc.Response{Error: jsonrpc.NewError(-32000, "boom", nil), ID: json.RawMessage(`"x"`)}
	b, _ := json.Marshal(r)
	got := string(b)
	if strings.Contains(got, `"result"`) {
		t.Errorf("error response contains result: %s", got)
	}
	if !strings.Contains(got, `"error"`) {
		t.Errorf("error response missing error: %s", got)
	}
}

func TestResponseMarshalNilIDBecomesNull(t *testing.T) {
	r := jsonrpc.Response{Result: json.RawMessage(`1`)}
	b, _ := json.Marshal(r)
	if !strings.Contains(string(b), `"id":null`) {
		t.Errorf("expected id:null, got %s", b)
	}
}

func TestResponseRoundTrip(t *testing.T) {
	in := `{"jsonrpc":"2.0","result":{"ok":true},"id":5}`
	var r jsonrpc.Response
	if err := json.Unmarshal([]byte(in), &r); err != nil {
		t.Fatal(err)
	}
	if string(r.ID) != "5" {
		t.Errorf("id = %s", r.ID)
	}
	if r.Error != nil {
		t.Errorf("unexpected error")
	}
	var res map[string]bool
	json.Unmarshal(r.Result, &res)
	if !res["ok"] {
		t.Errorf("result = %s", r.Result)
	}
}

func TestRequestIsNotification(t *testing.T) {
	notif := &jsonrpc.Request{Method: "x"}
	if !notif.IsNotification() {
		t.Error("expected notification")
	}
	req := &jsonrpc.Request{Method: "x", ID: json.RawMessage(`null`)}
	if req.IsNotification() {
		t.Error("explicit null id is not a notification")
	}
}

func TestErrorInterface(t *testing.T) {
	e := jsonrpc.NewError(-32000, "boom", nil)
	if !strings.Contains(e.Error(), "boom") || !strings.Contains(e.Error(), "-32000") {
		t.Errorf("Error() = %q", e.Error())
	}
	var nilErr *jsonrpc.Error
	if nilErr.Error() == "" {
		t.Error("nil error should return a placeholder string")
	}
}

func TestErrorConstructors(t *testing.T) {
	cases := []struct {
		err  *jsonrpc.Error
		code int
	}{
		{jsonrpc.ParseError("x"), jsonrpc.CodeParseError},
		{jsonrpc.InvalidRequest("x"), jsonrpc.CodeInvalidRequest},
		{jsonrpc.MethodNotFound("x"), jsonrpc.CodeMethodNotFound},
		{jsonrpc.InvalidParams("x"), jsonrpc.CodeInvalidParams},
		{jsonrpc.InternalError("x"), jsonrpc.CodeInternalError},
		{jsonrpc.ServerError("x"), jsonrpc.CodeServerError},
		{jsonrpc.Errorf(-32050, "n=%d", 5), -32050},
	}
	for _, tc := range cases {
		if tc.err.Code != tc.code {
			t.Errorf("code = %d, want %d", tc.err.Code, tc.code)
		}
	}
}
