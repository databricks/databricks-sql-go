package u2m

import (
	"html"
	"strings"
)

type SimplePage struct {
	Title   string
	Heading string
	Content string
	Action  ActionLink
	Code    string
}

type ActionLink struct {
	Label string
	Link  string
}

// The page is rendered with plain string concatenation and explicit HTML
// escaping instead of html/template. Reachable use of html/template (which
// relies on text/template's reflection-based evaluation) disables the Go
// linker's dead-code elimination for the entire binary, bloating any
// application that imports this driver. See issue #343.
const (
	pageHead = `<!DOCTYPE html SYSTEM "http://www.thymeleaf.org/dtd/xhtml1-strict-thymeleaf-4.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" xmlns:th="http://www.thymeleaf.org">

<head>
    <title>`

	pageAfterTitle = `</title>
    <link rel="preconnect" href="https://fonts.gstatic.com" />
    <link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:ital,wght@0,400;0,700;1,400&display=swap"
        rel="stylesheet" />

    <style>
        html,
        body {
            height: 100%;
        }

        body {
            font-family: "IBM Plex Sans";
            font-style: normal;
            font-size: 14px;
            margin: 0;
            padding: 0;
            height: 100%;
            width: 100%;
            background: #f5f6f6;
            align-items: center;
        }

        .root-container {
            display: flex;
            height: 100%;
            align-items: center;
            justify-content: center;
        }

        .info-container {
            width: 320px;
            box-shadow: 0px 2px 4px rgba(0, 0, 0, 0.1),
                0px 8px 25px rgba(0, 0, 0, 0.1);
            border-radius: 8px;
            display: flex;
            flex-direction: column;
            padding: 48px;
            background: #fff;
            justify-content: center;
            align-items: center;
            text-align: center;
            gap: 24px;
        }

        .logo {
            display: "block";
            max-width: 140px;
            max-height: 40px;
        }

        .title {
            font-weight: 600;
            font-size: 24px;
            line-height: 28px;
        }

        .content {
            width: 300px;
            font-size: 14px;
        }

        .button {
            display: flex;
            background: #191519;
            align-items: center;
            justify-content: center;
            height: 40px;
            width: 300px;
            border-radius: 4px;
            text-align: center;
            text-decoration: none;
            color: #ffffff !important;
        }
    </style>
</head>

<body>
    <div class="root-container">
        <div class="info-container">
            <img class="logo"
                src="https://www.databricks.com/wp-content/uploads/2022/06/db-nav-logo-stacked-white-desktop.svg" />
            <div class="title">`

	pageAfterHeading = `</div>
            <div class="content">`

	pageAfterContent = `</div>
`

	pageTail = `        </div>
    </div>
</body>

</html>`
)

func renderHTML(data SimplePage) (string, error) {
	var out strings.Builder
	out.WriteString(pageHead)
	out.WriteString(html.EscapeString(data.Title))
	out.WriteString(pageAfterTitle)
	out.WriteString(html.EscapeString(data.Heading))
	out.WriteString(pageAfterHeading)
	out.WriteString(html.EscapeString(data.Content))
	out.WriteString(pageAfterContent)
	if data.Action.Link != "" {
		out.WriteString(`            <a class="button" target="_blank" href="`)
		out.WriteString(html.EscapeString(data.Action.Link))
		out.WriteString(`">`)
		out.WriteString(html.EscapeString(data.Action.Label))
		out.WriteString("</a>\n")
	}
	if data.Code != "" {
		out.WriteString("            <code>")
		out.WriteString(html.EscapeString(data.Code))
		out.WriteString("</code>\n")
	}
	out.WriteString(pageTail)
	return out.String(), nil
}

func infoHTML(title, content string) string {
	data := SimplePage{
		Title:   "Authentication Success",
		Heading: title,
		Content: content,
	}
	out, _ := renderHTML(data)
	return out
}

func errorHTML(msg string) string {
	data := SimplePage{
		Title:   "Authentication Error",
		Heading: "Ooops!",
		Content: "Sorry, Databricks could not authenticate to your account due to some server errors. Please try it later.",
		Code:    msg,
	}
	out, _ := renderHTML(data)
	return out
}
