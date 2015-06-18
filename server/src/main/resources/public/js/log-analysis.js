$(document).ready(function(){
  $('#myTab a:first').tab('show');
  $('pre code').each(function(i, block) {
    hljs.highlightBlock(block);
  });
  $("div#myModal .close-reveal-modal").click(function(){
   // window.location.href="/";
  });
  $('.hide_list li:lt(5)').show();
  if( $('.hide_list li').size() > 5){
   $('.show_more').show();
   $('.show_less').show();
  }
  $('.show_more').click(function(){
  $('.hide_list li:gt(5)').show();
  });
  $('.show_less').click(function(){
  $('.hide_list li:gt(5)').hide();
  });
 });

function getHeight(size){
  if(size < 3 ) return 180;
  else if( size > 2 && size < 9) return 450;
  else if( size > 8 && size < 40) return 700;
  else if( size > 39 && size < 70) return 1000;
  else return 1300;
}

function histogram(originData, taskType, timeType){
  var data = originData;
  var minST = d3.min(data, function(d) {
                  return d.startTime ;
                });

  var maxFT = d3.max(data, function(d) {
            return d.finishTime ;
          });
  var minX = minST % 1000000000

  for( var d = data.length ; d-- ;){
    if(taskType == "MAP" && data[d].tasktype == "REDUCE") {
      data.splice(d, 1);
    }
    if(taskType == "REDUCE" && data[d].tasktype == "MAP"){
      data.splice(d, 1);
    }
  }

  // compute a appropriately initial height
  var initHeight = getHeight(data.length);
 // var globalWid = 750;
  var margin = {top: 30, right: 20, bottom: 30, left: 200},
      width = 750 - margin.right - margin.left,
      height = initHeight - margin.top - margin.bottom;
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


  var svg = d3.select("#"+taskType).append("svg")
      .attr("width", width + margin.left + margin.right)
      .attr("height", height + margin.top + margin.bottom)
      .append("g")
      .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

   var startTimePre = 0, finishTimePre = 0;
   y.domain(data.map(function(d) { return d.taskid; }));

   x.domain([0
    ,d3.max(data, function(d) {
      return (d.finishTime % 1000000000 - minX) / 1000 +100;
    })]);

  svg.append("g")
    .attr("class", "x axis")
    .call(xAxis)
    .append("text")
    .attr("x", width-110)
    .attr("dy", ".91em")
    .style("font" ,"12px sans-serif")
    .style("text-anchor", "begin")
    .text("Time(s)");

  svg.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0, 0)")
    .call(xAxis);


  svg.append("g")
    .attr("class", "y axis")
    .attr("transform", "translate(0, 0)")
    .call(yAxis);


  svg.selectAll(".bar")
    .data(data)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("x", function(d) {
      return (width / ((maxFT % 1000000000 - minX)/1000))*(((d.startTime % 1000000000) - minX)/1000) + 5;
    })
    .attr("width",function(d) {
      var wd = (d.finishTime % 1000000000) - (d.startTime % 1000000000) ;
      return wd / 1000;
    })
    .attr("y", function(d) { return y(d.taskid); })
    .attr("height", y.rangeBand())
    .style("fill", function(d) {
        return d.tasktype == "MAP" ? "rgb(149,162,93)" : "rgb(124,92,163)";
      })
    .on("mouseover", hyper_mouse)
    .on("click", hyper)
    .append("text")
    .attr("x", width-110)
    .attr("dy", ".91em")
    .style("font" ,"12px sans-serif")
    .style("text-anchor", "begin")
    .text("Time");


  d3.select("#"+taskType + " input")
    .on("change", change);

  var sortTimeout = setTimeout(function() {
      d3.select("input").property("checked", false).each(change);
   }, 2000);

  function hyper(d){
    location.href="/task/" + d.taskid;
  }

  function hyper_mouse(d){
    $('.bar').hover(function() {
       $(this).css('cursor','pointer');
    });
  }

  function change() {
   // alert(this.checked);
    clearTimeout(sortTimeout);
    // Copy-on-write since tweens are evaluated after a delay.
    var x0 = y.domain(data.sort(this.checked
      ? function(a, b) {
        return ((a.finishTime % 1000000000) - (a.startTime % 1000000000)) -
          ((b.finishTime % 1000000000) - (b.startTime % 1000000000));
      }
      : function(a, b) {
        return d3.ascending((a.startTime % 1000000000) - minX,
         (b.startTime % 1000000000) - minX);
      })
      .map(function(d) { return d.taskid; }))
      .copy();

    svg.selectAll(".bar")
      .sort(function(a, b) {
        return x0(a.taskid) - x0(b.taskid);
      });

    var transition = svg.transition().duration(750),
        delay = function(d, i) { return i * 50; };

    transition.selectAll(".bar")
      .delay(delay)
      .attr("y", function(d) { return x0(d.taskid); });

    transition.select(".y.axis")
      .call(yAxis)
      .selectAll("g")
      .delay(delay);
  }
}



