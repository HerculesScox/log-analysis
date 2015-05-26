function genGraph(originalVar, markVar){
  $(document).ready(function() {
    var g = new Graph();
    var render1 = function(r, n) {
      var ellipse = r.ellipse(0, 0, 21, 14).attr({fill: "#d3d3d3", stroke: "#d3d3d3", "stroke-width": 1});
      /* set DOM node ID */
      ellipse.node.id = n.label || n.id;
      var set = r.set().
                push(ellipse).
                push(r.text(0, 30, n.label || n.id));
      return set;
    };

    var render2 = function(r, n) {
      var ellipse = r.ellipse(0, 0, 21, 14).attr({fill: "#7cfc00", stroke: "#7cfc00", "stroke-width": 1});
      /* set DOM node ID */
      ellipse.node.id = n.label || n.id;
      var set = r.set().
                push(ellipse).
                push(r.text(0, 30, n.label || n.id));
      return set;
    };

    g.edgeFactory.template.style.directed = true;
    /* add node into graph */
    for(var key in originalVar){
      if( $.inArray(key, markVar) == -1) {
        g.addNode(key, {render : render1});
      }else{
        g.addNode(key, {render : render2});
      }
      var values = originalVar[key].split(",");
      for(var value in values){
        if($.inArray(values[value], markVar) == -1) {
          g.addNode(values[value], {render : render1});
        }else{
          g.addNode(values[value], {render : render2});
        }
      }
    }

    /* connect nodes with edges */
    for(var key in originalVar){
      var values = originalVar[key].split(",");
      if(values.length > 1){
        for(var value in values){
           g.addEdge(key, values[value]);
        }
      }else{
        g.addEdge(key, originalVar[key]);
      }
    }
    /* layout the graph using the Spring layout implementation */
    var layouter = new Graph.Layout.Spring(g);
    layouter.layout();

    /* draw the graph using the RaphaelJS draw implementation */
    var renderer = new Graph.Renderer.Raphael('canvas', g, 500, 450);
    renderer.draw();
  });


}

 $(document).ready(function() {
  $("div#myModal .close-reveal-modal").click(function(){
    window.location.href="/";
  })
 });