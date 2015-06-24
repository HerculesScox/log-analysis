<#include "header.ftl">
    <script type="text/javascript" src="/js/jquery-1.4.2.min.js"></script>
    <script type="text/javascript" src="/js/d3.v3.min.js"></script>
    <script type="text/javascript" src="/js/log-analysis.js"></script>
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        Task ID prefix&raquo; <small style="font-size:13px;padding-left:5px">${taskid}</small>
        <div class="tab-content">
          <div role="tabpanel" class="tab-pane active" id="task_IO">
              <script>
                taskIOMap(${data}, ${links});
              </script>
            </div>
          </div>
        </div>
    </div>
<#include "footer.ftl">
