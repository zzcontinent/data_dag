str_template = '''<!doctype html>

<meta charset="utf-8">
<title>%s</title>

<link rel="stylesheet" href="demo.css">
<script src="https://d3js.org/d3.v4.min.js" charset="utf-8"></script>
<script src="../dagre-d3.js"></script>

<!-- Pull in JQuery dependencies -->
<link rel="stylesheet" href="tipsy.css">
<script src="https://code.jquery.com/jquery-1.9.1.min.js"></script>
<script src="tipsy.js"></script>

<style id="css">
  text {
    font-weight: 300;
    font-family: "Helvetica Neue", Helvetica, Arial, sans-serf;
    font-size: 14px;
  }

  .node rect {
    stroke: #333;
    fill: #fff;
  }

  .edgePath path {
    stroke: #333;
    fill: #333;
    stroke-width: 1.5px;
  }

  .node text {
    pointer-events: none;
  }

  /* This styles the title of the tooltip */
  .tipsy .name {
    font-size: 1.5em;
    font-weight: bold;
    color: #60b1fc;
    margin: 0;
  }

  /* This styles the body of the tooltip */
  .tipsy .description {
    font-size: 1.2em;
  }

</style>

%s
'''

str_header = '''<h1>%s</h1>'''
str_svg = '''<svg width=960 height=600 id="%s"></svg>'''
str_script = '''<script>
  // Create a new directed graph
  var g = new dagreD3.graphlib.Graph().setGraph({});

  // States and transitions from RFC 793
  var states = {
    %s
  };

  // Add states to the graph, set labels, and style
  Object.keys(states).forEach(function (state) {
    var value = states[state];
    value.label = state;
    value.rx = value.ry = 5;
    g.setNode(state, value);
  });

  // Set up the edges
  %s


  // Create the renderer
  var render = new dagreD3.render();

  // Set up an SVG group so that we can translate the final graph.
  var svg = d3.select("#%s"),
    %s = svg.append("g");

  // Set up zoom support
  var zoom = d3.zoom()
    .on("zoom", function () {
      %s.attr("transform", d3.event.transform);
    });
  svg.call(zoom);

  // Simple function to style the tooltip for the given node.
  var styleTooltip = function (name, description) {
    return "<p class='name'>" + name + "</p><p class='description' style='color:orange; width:100%%; height:100%%; display:inline-block' >" + description + "</p>";
  };

  // Run the renderer. This is what draws the final graph.
  render(%s, g);

  %s.selectAll("g.node")
    .attr("title", function (v) { return styleTooltip(v, g.node(v).description) })
    .each(function (v) { $(this).tipsy({ gravity: "w", opacity: 1, html: true }); });

  // Center the graph
  var initialScale = 0.75;
  svg.call(zoom.transform, d3.zoomIdentity.translate((svg.attr("width") - g.graph().width * initialScale) / 2, 20).scale(initialScale));

  svg.attr('height', g.graph().height * initialScale + 40);
</script>
'''
