package view

import (
	"encoding/json"
	"fmt"
	"image/color"
	"image/color/palette"
	"io"
	"math"
	"sync"

	"github.com/Miniand/langtrend/db"
	"github.com/Miniand/langtrend/github"
	"github.com/Miniand/langtrend/period"
)

var languageShowMutex = &sync.Mutex{}

var languageShowT = `{{template "header" .HeaderData}}
<div style="width:62%;margin:0 auto;">
	<div style="position:relative; top: 30px; left: 70px;">
		<div id="legend" style="position:absolute;"></div>
	</div>
	<canvas data-chart="{{.ChartType}}" data-chart-legend="#legend"
		data-chart-options="{&quot;scaleBeginAtZero&quot;:true}"
		data-chart-data="{{.ChartData}}"></canvas>
</div>
{{template "footer"}}`

type LanguageShowData struct {
	Name       string
	Counts     [][]db.Aggregate
	ChartData  string
	HeaderData HeaderData
	Metric     string
	ChartType  string
}

func (lsd LanguageShowData) Value(a db.Aggregate) float64 {
	switch lsd.Metric {
	case "total":
		return float64(a.Total)
	case "rank":
		return float64(a.Rank)
	default:
		return math.Floor(a.Ratio*math.Pow(10, 6)) / math.Pow(10, 4)
	}
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
	per, err := period.FromIdentifier(data.Counts[0][0].Type)
	if err != nil {
		return fmt.Errorf("could not get period, %s", err)
	}
	for i, lang := range data.Counts {
		values := make([]float64, len(lang))
		for j, count := range lang {
			if i == 0 {
				per.SetReference(count.Start)
				labels[j] = per.String()
			}
			if j == 0 {
				datasets[i].Label = count.Language
				col := palette.WebSafe[(i*28)%216].(color.RGBA)
				if c, ok := github.LanguageColors[count.Language]; ok {
					col = c
				}
				datasets[i].FillColor = fmt.Sprintf(
					"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 0.0)
				datasets[i].StrokeColor = fmt.Sprintf(
					"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 1.0)
				datasets[i].PointColor = fmt.Sprintf(
					"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 1.0)
				datasets[i].PointStrokeColor = "#fff"
				datasets[i].PointHighlightFill = "#fff"
				datasets[i].PointHighlightStroke = fmt.Sprintf(
					"rgba(%d,%d,%d,%f)", col.R, col.G, col.B, 1.0)
			}
			values[j] = data.Value(count)
		}
		datasets[i].Data = values
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
