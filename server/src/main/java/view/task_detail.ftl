<#include "header.ftl">
    <script type="text/javascript" src="/js/jquery-1.4.2.min.js"></script>
    <script type="text/javascript" src="/js/log-analysis.js"></script>
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        <div class="table-responsive">
            <table  class="table  table-hover  table-striped table-bordered">
                <tr>
                  <td  width="15%"><strong>Task ID</strong></td>
                  <td>${taskid}</td>
                </tr>
                <tr>
                  <td><strong>Task Type</strong></td>
                  <td>${type}</td>
                </tr>
                <tr>
                  <td><strong>Start Time</strong></td>
                  <td>${startTime?number_to_datetime}</td>
                </tr>
                <tr>
                  <td><strong>Finish Time</strong></td>
                  <td>${finishTime?number_to_datetime}</td>
                </tr>
                <tr>
                  <td><strong>Status</strong></td>
                  <td>${status}</td>
                </tr>
                <#if splitLocation?length != 0>
                  <tr>
                    <td><strong>Split Location</strong></td>
                    <td>${splitLocation}</td>
                  </tr>
                </#if>
                <#if error?length != 0>
                  <tr>
                    <td><strong>Error</strong></td>
                    <td>${error}</td>
                  </tr>
                </#if>
                <tr>
                  <td ><strong>Attempt Task</strong></td>
                  <td>
                    <table  class="table table-bordered">
                      <#list attemptTasks?keys as ats>
                        <tr>
                          <strong>[${ats_index}]&nbsp;&nbsp;${ats}</strong>
                          <#assign at = attemptTasks[ats]>
                          <ol style="list-style:disc;padding-left:50px;">
                            <#list at?keys as content>
                              <li>${content}:
                                <#if at[content]??>
                                  <#if at[content]?is_number&&(at[content]>0)>
                                    ${at[content]?number_to_datetime}
                                  <#else>
                                     ${at[content]}
                                  </#if>
                                <#else>
                                  NO
                                </#if>
                              </li>
                            </#list>
                          </ol>
                        </tr>
                      </#list>
                    </table>
                   </td>
                </tr>
                <#if type == "MAP">
                  <tr>
                    <td><strong>Input Format</strong></td>
                    <td>
                      ${inputFormat}
                     </td>
                  </tr>
                  <tr>
                    <td><strong>Input File</strong></td>
                    <td>
                      <ol style="list-style:disc;padding-left:20px;">
                        <#list splitFiles as at>
                          <li>${at?replace("[","")?replace("]","")}</li>
                        </#list>
                      <ol>
                    </td>
                  </tr>
                  <tr>
                    <td><strong>Output To</strong></td>
                    <td>
                       <ul class="hide_list" style="list-style:disc;padding-left:20px;">
                         <#list outputTo as tid>
                           <li><a href="/task/${tid}">${tid}</a></li>
                         </#list>
                          <div class="show_more"> more...</div>
                          <div class="show_less"> less...</div>
                      </ul
                     </td>
                  </tr>
                <#else>
                   <tr>
                     <td><strong>Input MapTasks</strong></td>
                     <td>
                      <ul class="hide_list" style="list-style:disc;padding-left:20px;">
                       <#list inputMapTasks as at>
                         <#assign mtask = at?replace("[","")?replace("]","")>
                         <li><a href="/task/${mtask?replace(" ","")}"> ${mtask}</a></li>
                       </#list>
                        <div class="show_more"> more...</div>
                        <div class="show_less"> less...</div>
                      </ul>
                     </td>
                   </tr>
                </#if>
                <tr>
                  <td style="vertical-align:middle"><strong>Operator Tree</strong></td>
                  <td>
                    <table class="table" style="margin-bottom:-2px;border-bottom:1px solid #ddd;">
                      <#list operatorTree?keys as at>
                        <tr>
                          <#assign op = operatorTree[at]?replace("<", "")?replace(">", "")?split(" ")>
                          <td width="10%"> ${op[0]} </td>
                          <td> ${op[1]} ${op[2]}
                            <#if op[3]??> </br><em>FS Path:</em> ${op[3]}</#if>
                          </td>
                        </tr>
                      </#list>
                    </table>
                  </td>
                </tr>
                <tr>
                  <td style="vertical-align:middle"><strong>Counter</strong></td>
                  <td>
                  <strong>TASK COUNTER:</strong>
                    <ul style="list-style:circle;padding-left:60px;">
                      <#list Counter?keys as c>
                        <li>${c}: ${Counter[c]}</li>
                      </#list>
                     </ul>
                  </td>
                </tr>
            </table>
        </div>
    </div>
<#include "footer.ftl">