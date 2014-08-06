package view

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/Miniand/langtrend/db"
)

var languageShowMutex = &sync.Mutex{}

var languageShowT = `{{template "header" .HeaderData}}
<h1>{{.Name}}</h1>

<p>
{{range .Counts}}
{{.Count}}
{{end}}
</p>
<canvas width="1600" height="700" data-chart="Line"
	data-chart-data="{{.ChartData}}"></canvas>
{{template "footer"}}`

type LanguageShowData struct {
	Name       string
	Counts     []db.LanguageDateCount
	ChartData  string
	HeaderData HeaderData
}

func LanguageShow(w io.Writer, data LanguageShowData) error {
	languageShowMutex.Lock()
	t := baseT.Lookup("languageShow")
	if t == nil {
		var err error
		if t, err = baseT.New("languageShow").Parse(languageShowT); err != nil {
			languageShowMutex.Unlock()
			return err
		}
	}
	languageShowMutex.Unlock()
	labels := make([]string, len(data.Counts))
	values := make([]int, len(data.Counts))
	for i, c := range data.Counts {
		labels[i] = c.Date.Format("2006-01-02")
		values[i] = c.Count
	}
	chartData := map[string]interface{}{
		"labels": labels,
		"datasets": []map[string]interface{}{
			{
				"label":                "My First dataset",
				"fillColor":            "rgba(220,220,220,0.2)",
				"strokeColor":          "rgba(220,220,220,1)",
				"pointColor":           "rgba(220,220,220,1)",
				"pointStrokeColor":     "#fff",
				"pointHighlightFill":   "#fff",
				"pointHighlightStroke": "rgba(220,220,220,1)",
				"data":                 values,
			},
		},
	}
	j, err := json.Marshal(chartData)
	if err != nil {
		return err
	}
	data.ChartData = string(j)
	return t.Execute(w, data)
}
