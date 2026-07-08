package u2m

import (
	"strings"
	"testing"
)

func TestRenderHTML(t *testing.T) {
	tests := []struct {
		name         string
		data         SimplePage
		wantContains []string
		wantAbsent   []string
	}{
		{
			name: "basic fields are rendered and escaped",
			data: SimplePage{
				Title:   "My <Title>",
				Heading: "Heading & more",
				Content: "some \"content\"",
			},
			wantContains: []string{
				"<title>My &lt;Title&gt;</title>",
				`<div class="title">Heading &amp; more</div>`,
				`<div class="content">some &#34;content&#34;</div>`,
			},
			wantAbsent: []string{
				"<code>",
				`<a class="button"`,
			},
		},
		{
			name: "action link block renders when link is set",
			data: SimplePage{
				Title:   "t",
				Heading: "h",
				Content: "c",
				Action:  ActionLink{Label: "Click <me>", Link: "https://example.com/?a=1&b=2"},
			},
			wantContains: []string{
				`href="https://example.com/?a=1&amp;b=2"`,
				">Click &lt;me&gt;</a>",
			},
			wantAbsent: []string{
				"<code>",
			},
		},
		{
			name: "code block renders when code is set",
			data: SimplePage{
				Title:   "t",
				Heading: "h",
				Content: "c",
				Code:    "err <500>",
			},
			wantContains: []string{
				"<code>err &lt;500&gt;</code>",
			},
			wantAbsent: []string{
				`<a class="button"`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := renderHTML(tt.data)
			if err != nil {
				t.Fatalf("renderHTML returned error: %v", err)
			}
			for _, want := range tt.wantContains {
				if !strings.Contains(out, want) {
					t.Errorf("output missing %q\nfull output:\n%s", want, out)
				}
			}
			for _, absent := range tt.wantAbsent {
				if strings.Contains(out, absent) {
					t.Errorf("output unexpectedly contains %q\nfull output:\n%s", absent, out)
				}
			}
			// Every page must be a well-formed, complete document.
			if !strings.HasPrefix(out, "<!DOCTYPE html") {
				t.Errorf("output does not start with doctype:\n%s", out)
			}
			if !strings.HasSuffix(out, "</html>") {
				t.Errorf("output does not end with </html>:\n%s", out)
			}
		})
	}
}

func TestInfoHTML(t *testing.T) {
	out := infoHTML("CLI Login Success", "You may close this window")
	if !strings.Contains(out, "<title>Authentication Success</title>") {
		t.Errorf("infoHTML missing expected title:\n%s", out)
	}
	if !strings.Contains(out, `<div class="title">CLI Login Success</div>`) {
		t.Errorf("infoHTML missing heading:\n%s", out)
	}
	if !strings.Contains(out, "You may close this window") {
		t.Errorf("infoHTML missing content:\n%s", out)
	}
	if strings.Contains(out, "<code>") {
		t.Errorf("infoHTML should not render a code block:\n%s", out)
	}
}

func TestErrorHTML(t *testing.T) {
	out := errorHTML("boom <script>")
	if !strings.Contains(out, "<title>Authentication Error</title>") {
		t.Errorf("errorHTML missing expected title:\n%s", out)
	}
	// The error message must be HTML-escaped, never rendered as raw markup.
	if !strings.Contains(out, "<code>boom &lt;script&gt;</code>") {
		t.Errorf("errorHTML did not escape the error code:\n%s", out)
	}
	if strings.Contains(out, "<script>") {
		t.Errorf("errorHTML leaked unescaped markup:\n%s", out)
	}
}
