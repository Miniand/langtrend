package view

import (
	"encoding/json"
	"fmt"
	"image/color"
	"image/color/palette"
	"io"
	"sync"

	"github.com/Miniand/langtrend/db"
)

var languageShowMutex = &sync.Mutex{}

var languageShowT = `{{template "header" .HeaderData}}
<div style="width:62%;margin:0 auto;">
	<div style="position:relative; top: 30px; left: 70px;">
		<div id="legend" style="position:absolute;"></div>
	</div>
	<canvas data-chart="Line" data-chart-legend="#legend"
		data-chart-data="{{.ChartData}}"></canvas>
</div>
{{template "footer"}}`

type LanguageShowData struct {
	Name       string
	Counts     [][]db.LanguageDateCount
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
	labels := make([]string, len(data.Counts[0]))
	datasets := make([]ChartData, len(data.Counts))
	for i, lang := range data.Counts {
		col := palette.WebSafe[i*28].(color.RGBA)
		datasets[i].FillColor = fmt.Sprintf(
			"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 0.2)
		datasets[i].StrokeColor = fmt.Sprintf(
			"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 1.0)
		datasets[i].PointColor = fmt.Sprintf(
			"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 1.0)
		datasets[i].PointStrokeColor = "#fff"
		datasets[i].PointHighlightFill = "#fff"
		datasets[i].PointHighlightStroke = fmt.Sprintf(
			"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 1.0)
		data := make([]float64, len(lang))
		for j, count := range lang {
			if i == 0 {
				labels[j] = count.Date.Format("2006-01-02")
			}
			if j == 0 {
				datasets[i].Label = count.Language
			}
			data[j] = float64(count.Count)
		}
		datasets[i].Data = data
	}

	chartData := map[string]interface{}{
		"labels":   labels,
		"datasets": datasets,
	}
	j, err := json.Marshal(chartData)
	if err != nil {
		return err
	}
	data.ChartData = string(j)
	return t.Execute(w, data)
}
