package view

import "html/template"

var baseT = template.Must(template.New("baseT").Parse(`
{{define "header"}}
<html>
<head>
<title>Lang Trend{{if .Title}} - {{.Title}}{{end}}</title>
<link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/pure/0.5.0/pure-min.css">
<script src="//cdnjs.cloudflare.com/ajax/libs/jquery/2.1.1/jquery.min.js"></script>
<script src="//cdnjs.cloudflare.com/ajax/libs/Chart.js/0.2.0/Chart.min.js"></script>
<!--[if lte IE 8]>
	<script src="excanvas.js"></script>
<![endif]-->
<body>
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
		c[type](data, options);
		el.attr('data-chart-initialised', true);
	});
}
$(document).ready(function() {
	initCharts();
});
</script>
{{end}}

{{define "footer"}}
</body>
</html>
{{end}}
`))

type HeaderData struct {
	Title string
}
