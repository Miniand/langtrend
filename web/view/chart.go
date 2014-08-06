package view

type ChartData struct {
	Label                string    `json:"label,omitempty"`
	Data                 []float64 `json:"data,omitempty"`
	Value                float64   `json:"value,omitempty"`
	Highlight            string    `json:"highlight,omitempty"`
	FillColor            string    `json:"fillColor,omitempty"`
	StrokeColor          string    `json:"strokeColor,omitempty"`
	HighlightFill        string    `json:"highlightFill,omitempty"`
	HighlightStroke      string    `json:"highlightStroke,omitempty"`
	PointColor           string    `json:"pointColor,omitempty"`
	PointStrokeColor     string    `json:"pointStrokeColor,omitempty"`
	PointHighlightFill   string    `json:"pointHighlightFill,omitempty"`
	PointHighlightStroke string    `json:"pointHighlightStroke,omitempty"`
}
