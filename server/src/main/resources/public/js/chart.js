function histogram(d){
  var margin = {top: 50, right: 20, bottom: 30, left: 40},
      width = 960 - margin.left - margin.right,
      height = 700 - margin.top - margin.bottom;

  var x = d3.scale.linear()
         .rangeRound([0, width]);
  var y = d3.scale.ordinal()
          .rangeRoundBands([0, height], .5, .3);

  var xAxis = d3.svg.axis()
      .scale(x)
      .orient("top")

  var yAxis = d3.svg.axis()
      .scale(y)
      .orient("left");



  var svg = d3.select("body").append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");


  var data =
    [
      {"letter" : "A" , "frequency" : 4167 , "tasktype" : "MAP"},
      {"letter" : "B" , "frequency" : 6167, "tasktype" : "MAP"},
      {"letter" : "C" , "frequency" : 3167, "tasktype" : "MAP"},
      {"letter" : "D" , "frequency" : 5127, "tasktype" : "REDUCE"},
      {"letter" : "E" , "frequency" : 2167, "tasktype" : "MAP"},
      {"letter" : "F" , "frequency" : 8167, "tasktype" : "REDUCE"},
      {"letter" : "G" , "frequency" : 3167, "tasktype" : "MAP"},
      {"letter" : "H" , "frequency" : 9167, "tasktype" : "REDUCE"}
   ];


   y.domain(data.map(function(d) { return d.letter; }));
   x.domain([0, d3.max(data, function(d) { return d.frequency; })]);

  svg.append("g")
    .attr("class", "x axis")
    .call(xAxis)
    .append("text")
    .attr("x", width-80)
    .attr("dy", ".81em")
    .style("font" ,"10px sans-serif")
    .style("text-anchor", "begin")
    .text("Frequency");

  svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate("+0 + ",0)")
    .call(xAxis);


  svg.append("g")
    .attr("class", "y axis")
    .attr("transform", "translate(0 ,0)")
    .call(yAxis);


  svg.selectAll(".bar")
    .data(data)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("x", 5)
    .attr("width",function(d) { return x(d.taskid);})
    .attr("y", function(d) { return y(d.letter); })
    .attr("height", y.rangeBand())
    .style("fill", d.tasktype == 'MAP' ? "fill: rgb(149,162,93);" : "fill: rgb(72,183,168);")
    .on("mouseover", hyper_mouse)
    .on("click", hyper);

  d3.select("input")
    .on("change", change);

  var sortTimeout = setTimeout(function() {
    d3.select("input").property("checked", false).each(change);
  }, 2000);
}

function hyper(d){
  location.href="#";
}

function hyper_mouse(d){
  $('.bar').hover(function() {
     $(this).css('cursor','pointer');
  });
}

 function change() {
    clearTimeout(sortTimeout);
    // Copy-on-write since tweens are evaluated after a delay.
    var x0 = y.domain(data.sort(this.checked
        ? function(a, b) { return b.frequency - a.frequency; }
        : function(a, b) { return d3.ascending(a.letter, b.letter); })
        .map(function(d) { return d.letter; }))
        .copy();

    svg.selectAll(".bar")
        .sort(function(a, b) { return x0(a.letter) - x0(b.letter); });

    var transition = svg.transition().duration(750),
        delay = function(d, i) { return i * 50; };

    transition.selectAll(".bar")
        .delay(delay)
        .attr("y", function(d) { return x0(d.letter); });

    transition.select(".y.axis")
        .call(yAxis)
      .selectAll("g")
        .delay(delay);
 }
