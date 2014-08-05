package view

import (
	"html/template"
	"io"

	"github.com/Miniand/langtrend/db"
)

var languageShowT = template.Must(template.New("home").Parse(
	`<h1>{{.Name}}</h1>

{{range .Counts}}
{{.Count}}
{{end}}`))

type LanguageShowData struct {
	Name   string
	Counts []db.LanguageDateCount
}

func LanguageShow(w io.Writer, data LanguageShowData) error {
	return languageShowT.Execute(w, data)
}
