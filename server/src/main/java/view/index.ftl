<#include "header.ftl">
    <link href="/css/highlight.css" rel="stylesheet">
    <script type="text/javascript" src="/js/jquery-1.4.2.min.js"></script>
    <script type="text/javascript" src="/js/viz.js"></script>
    <script type="text/javascript" src="/js/jquery.reveal.js"></script>
    <script type="text/javascript" src="/js/log-analysis.js"></script>
    <script type="text/javascript" src="/js/highlight.pack.js"></script>
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
      <table>
          <tr>
            <td width="69%"> <h2 class="sub-header">${subTitle}</h2></td>
            <td>
              <form action="/index_user" method="post" class="navbar-form navbar-left" role="search">
                <div class="form-group">
                  <input type="text" name="user" class="form-control" placeholder="Username">
                </div>
                <button type="submit" class="btn btn-info">Search</button>
              </form>
            </td>
          </tr>
       <table>
        <div class="table-responsive">
            <table  class="table table-hover ">
                <thead>
                <tr>
                    <th style="text-align:center;" width="5%">NO.</th>
                    <th style="text-align:center;" width="7%">USERNAME</th>
                    <th style="text-align:center;" width="50%">QUERY</th>
                    <th style="text-align:center;" width="26%">JOB DEPENDENCY</th>
                    <th style="text-align:center;" >JOBS</th>
                </tr>
                </thead>
                <tbody>
                <script> stagesMaps = []; markeds = [];</script>
                <#list allInfo as info>
                    <script> var stagesMap = {}; var marked = [];</script>
                    <tr>
                        <td style="text-align:center;vertical-align:middle">${info['number']}</td>
                        <td style="text-align:center;vertical-align:middle">${info['username']}</td>
                        <td >
                         <pre><code class="SQL">${info['queryString']?replace("^\\s+", "", "r")}</code></pre>
                        </td>
                        <td>
                          <p class="graph_button" onClick="genGraph(${info['number']})">
                             ${info['launchTime']?number_to_datetime}
                            <a href="#"  style="color:green;" data-reveal-id="myModal" data-animation="none" onClick="genGraph(stagesMaps[${info['number']}], markeds[${info['number']}]);">
                          (graph)</a></p>
                         <#assign json = info['jobDependency']?eval>
                         <#assign jts = info['stageToJobID']>
                         <#assign stages = jts?keys>
                         <#list stages as stage>
                          <#assign res = "'" + stage + "'">
                          <script>marked[${stage_index}] = ${res};</script>
                         </#list>

                         <#list json?keys as key >
                            <script>
                              stagesMap[${key}] = ${json[key]};
                            </script>
                            <#assign s1 = key?replace("\"","")?replace("\\(.*\\)","",'r')>
                            <#assign s2 = json[key]?replace("\"","")?replace("\\(.*\\)","",'r')>

                            <small>
                            <#if jts[s1]??><a href='/job/${jts[s1]}'>${s1}</a><#else>${s1}</#if>
                            ->
                            <#if jts[s2]??><a href="/job/${jts[s2]}">${s2}</a><#else>${s2}</#if>
                            </small>
                            </br>
                         </#list>
                         <script>stagesMaps[${info['number']}] = stagesMap; markeds[${info['number']}] = marked ;</script>
                        </td>
                        <td style="text-align:center;vertical-align:middle">
                          <a href="/jobs/${info['workflowID']}">${info["jobAmount"]}</a>
                          </br>
                        </td>
                    </tr>
                </#list>
                </tbody>
            </table>
             <div style="text-align:center">
                <ul class="pagination pagination-small" style="padding-left:0px;margin-left:auto;margin-right:auto;">
                  <li>
                    <#if page == 1>
                      <a class="disable" href="#" aria-label="Previous">
                    <#else>
                      <a href="/page/${page-1}/user/${user}" aria-label="Previous">
                    </#if>
                      <span aria-hidden="true">&laquo;</span>
                    </a>
                  </li>
                  <#assign pages = (count / maxSize)?ceiling>
                  <#list 1..pages as page>
                      <li><a href="/page/${page}/user/${user}">${page}</a></li>
                  </#list>
                  <li>
                    <#if page == pages>
                      <a class="disable" href="#" aria-label="Next">
                    <#else>
                      <a href="/page/${page+1}/user/${user}" aria-label="Next">
                    </#if>
                      <span aria-hidden="true">&raquo;</span>
                    </a>
                  </li>
                </ul>
              </div>


        </div>
        <div id="myModal" class="reveal-modal">
            <div id="canvas"></div>
            <a class="close-reveal-modal">&#215;</a>
        </div>
    </div>
<#include "footer.ftl">
<#list allInfo as info>
  <script type="text/vnd.grpahviz" id="${info['number']}">
    ${info['svg']}
  </script>
</#list>

<script>
function inspect(s) {
    return "<pre>" + s.replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/\"/g, "&quot;") + "</pre>"
  }

function src(id) {
  return document.getElementById(id).innerHTML;
}

function viz(id, format, engine) {
  var result;
  try {
    result = Viz(src(id), format, engine);
       return result;
  } catch(e) {
    return inspect(e.toString());
  }
}

function genGraph(id){
  document.getElementById('canvas').innerHTML = viz(id,"svg");
}
</script>