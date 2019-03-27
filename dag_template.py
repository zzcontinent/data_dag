str_template = '''<!doctype html>

<meta charset="utf-8">
<title>%s</title>

<link rel="stylesheet" href="demo.css">
<script src="https://d3js.org/d3.v4.min.js" charset="utf-8"></script>
<script src="./dagre-d3.js"></script>

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

str_header = '''<h3>%(HEADER)s</h3>'''
str_svg = '''<svg width=960 height=800 id="%(SVG_ID)s"></svg>'''
str_script = '''<script>
  var highlight_color = "#000000";
  var upstream_color = "#0000FF";
  var downstream_color = "#0000FF"
  var fill_default_color = "rgb(240, 237, 228)"
  var fill_upstream_color = "orange"
  var fill_downstream_color = "orange"
  var initialStrokeWidth = '3px';
  
  // Create a new directed graph
  var %(GRAPH)s = new dagreD3.graphlib.Graph().setGraph({});

  // States and transitions from RFC 793
  var states = {
    %(NODES_TEMPLATE)s
  };

  // Add states to the graph, set labels, and style
  Object.keys(states).forEach(function (state) {
    var value = states[state];
    value.rx = value.ry = 5;
    %(GRAPH)s.setNode(state, value);
  });

  // Set up the edges
  %(EDGE_TEMPLATE)s


  // Create the renderer
  var render = new dagreD3.render();

  // Set up an SVG group so that we can translate the final graph.
  var svg = d3.select("#%(SVG_ID)s"),
    %(INNER_ID)s = svg.append("g");

  // Set up zoom support
  var zoom = d3.zoom()
    .on("zoom", function () {
      %(INNER_ID)s.attr("transform", d3.event.transform);
    });
  svg.call(zoom);

  // Simple function to style the tooltip for the given node.
  var styleTooltip = function (name, description) {
    return "<p class='name'>" + name + "</p><p class='description' style='color:orange; width:100%%; height:100%%; display:inline-block' >" + description +"<br>" + "</p>";
  };

  // Run the renderer. This is what draws the final graph.
  render(%(INNER_ID)s, %(GRAPH)s);

  %(INNER_ID)s.selectAll("g.node")
    .attr("title", function (v) { return styleTooltip(%(GRAPH)s.node(v).label, %(GRAPH)s.node(v).description) })
    .each(function (v) { $(this).tipsy({ gravity: "w", opacity: 1, html: true }); });

  %(INNER_ID)s.selectAll("g.node").style("stroke-width", initialStrokeWidth);

  // function highlight nodes
  %(INNER_ID)s.selectAll("g.node").on("mouseover", function (d) {
    %(INNER_ID)s.selectAll("rect").style("stroke", highlight_color);
    %(GRAPH)s.predecessors(d).forEach(function (nodeid) {
      my_node = %(INNER_ID)s.select('#' + nodeid + ' rect');
      my_node.style("stroke", fill_upstream_color);
    })

    %(GRAPH)s.successors(d).forEach(function (nodeid) {
      my_node = %(INNER_ID)s.select('#' + nodeid + ' rect');
      my_node.style("stroke", fill_downstream_color);
    })
  });

  %(INNER_ID)s.selectAll("g.node").on("mouseout", function (d) {
    %(INNER_ID)s.selectAll("rect").style("stroke", null);
    %(GRAPH)s.predecessors(d).forEach(function (nodeid) {
      my_node = %(INNER_ID)s.select('#' + nodeid + ' rect');
      my_node.style("stroke", null);
    })

    %(GRAPH)s.successors(d).forEach(function (nodeid) {
      my_node = %(INNER_ID)s.select('#' + nodeid + ' rect');
      my_node.style("stroke", null);
    })
  });
  
  // Center the graph
  var initialScale = 0.75;
  svg.call(zoom.transform, d3.zoomIdentity.translate((svg.attr("width") - %(GRAPH)s.graph().width * initialScale) / 2, 20).scale(initialScale));

  svg.attr('height', %(GRAPH)s.graph().height * initialScale + 40);
</script>
'''
