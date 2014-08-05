package view

import (
	"html/template"
	"io"
)

var homeT = template.Must(template.New("home").Parse(`<h1>MOO</h1>`))

type HomeData struct{}

func Home(w io.Writer, data HomeData) error {
	return homeT.Execute(w, data)
}
