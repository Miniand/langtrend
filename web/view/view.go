package view

import (
	"fmt"
	"html/template"
)

var baseT = template.Must(template.New("baseT").Parse(fmt.Sprintf(`
{{define "header"}}
<html>
<head>
<title>Lang Trend{{if .Title}} - {{.Title}}{{end}}</title>
<link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/pure/0.5.0/pure-min.css">
<script src="//cdnjs.cloudflare.com/ajax/libs/jquery/2.1.1/jquery.min.js"></script>
<script>%s</script>
<!--[if lte IE 8]>
	<script src="excanvas.js"></script>
<![endif]-->
<script>
function initCharts() {
	$('[data-chart]:not([data-chart-initialised])').each(function() {
		var el = $(this);
		var type = el.attr('data-chart');
		var data = $.parseJSON(el.attr('data-chart-data'));
		var options = el.attr('data-chart-options');
		if (options) {
			options = $.parseJSON(options);
		}
		var c = new Chart(this.getContext("2d"));
		var created = c[type](data, options);
		if (el.attr('data-chart-legend')) {
			$(el.attr('data-chart-legend')).html(created.generateLegend());
		}
		el.attr('data-chart-initialised', true);
	});
}
$(document).ready(function() {
	Chart.defaults.global.animation = false;
	Chart.defaults.global.responsive = true;
	initCharts();
});
</script>
<style>
ul.line-legend {
	cursor: default;
	list-style-type: none;
	background-color: rgba(255,255,255,0.5);
	padding: 0;
	margin: 0;
	opacity: 1;
	transition: opacity 0.2s;
}
ul.line-legend:hover {
	opacity: 0.2;
	transition: 0.2s;
}
.line-legend span {
	display: inline-block;
	height: 1em;
	width: 1em;
	margin-right: 0.3em;
}
</style>
<body>
{{end}}

{{define "footer"}}
</body>
</html>
{{end}}
`, chartjs)))

type HeaderData struct {
	Title string
}
