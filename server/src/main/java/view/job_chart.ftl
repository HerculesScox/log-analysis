<#include "header.ftl">
    <script type="text/javascript" src="/js/jquery-1.4.2.min.js"></script>
    <script type="text/javascript" src="/js/d3.v3.min.js"></script>
    <script type="text/javascript" src="/js/log-analysis.js"></script>
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        <ul class="nav nav-tabs" role="tablist" id="myTab">
          <li role="presentation" class="active"><a href="#ALL" aria-controls="ALL" role="tab" data-toggle="tab">ALL</a></li>
          <li role="presentation"><a href="#MAP" aria-controls="MAP" role="tab" data-toggle="tab">Map Task</a></li>
          <li role="presentation"><a href="#REDUCE" aria-controls="REDUCE" role="tab" data-toggle="tab">Reduce Task</a></li>
        </ul>

        <div class="tab-content">
          <div role="tabpanel" class="tab-pane active" id="ALL">
            <div class="chart_label"> <label><input type="checkbox"/> sort by running time</label></div>
              <script>
                var data = ${data};
                histogram(data, "ALL", "startTime")
              </script>
            </div>
            <div role="tabpanel" class="tab-pane" id="MAP">
              <div class="chart_label"> <label><input type="checkbox"/> Sort </label></div>
              <script>
                var data = ${data};
                histogram(data, "MAP", "startTime")
              </script>
            </div>
            <div role="tabpanel" class="tab-pane" id="REDUCE">
               <div class="chart_label"> <label><input type="checkbox"/> Sort </label></div>
               <script>
                 var data = ${data};
                 histogram(data, "REDUCE", "startTime")
               </script>
            </div>
          </div>
        </div>
    </div>
<#include "footer.ftl">
