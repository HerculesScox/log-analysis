<#include "header.ftl">
    <script type="text/javascript" src="/js/jquery-1.4.2.min.js"></script>
    <script type="text/javascript" src="/js/log-analysis.js"></script>
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">
          ${subTitle}
        </h2>

        <div class="table-responsive">
            <table  class="table  table-hover  table-striped table-bordered">
                <tbody>
                <tr>
                  <td  width="16%"><strong>JOB ID</strong></td>
                  <td>${jobid}</td>
                </tr>
                <tr>
                  <td><strong>Stage</strong></td>
                  <td>${stage}</td>
                </tr>
                <tr>
                  <td ><strong>Launch Time</strong></td>
                  <td>
                    ${json["launchTime"]?number_to_datetime}
                   </td>
                </tr>
                <tr>
                  <td><strong>Submit Time</strong></td>
                  <td>
                    ${json["submitTime"]?number_to_datetime}
                   </td>
                </tr>
                <tr>
                  <td><strong>Task Time Chart</strong></td>
                  <td>
                    <em style="font-size:13px;padding-left:30px"><a href="/jobchart/${jobid}">Entry<a></em>
                  </td>
                </tr>
                <tr>
                  <td><strong>Task IO Chart</strong></td>
                  <td>
                    <em style="font-size:13px;padding-left:30px"><a href="/mapoutput/${jobid}">Entry<a></em>
                  </td>
                </tr>
                <tr>
                  <td><strong><a href="/job/${jobid}/ops/MAP">Map Ops</a></strong></td>
                  <td>
                    <#if mapOps??>
                       <#list mapOps?keys as kk>
                          ${kk}(${mapOps[kk]} rows) &nbsp;
                        </#list>
                    </#if>
                   </td>
                </tr>
                <tr>
                  <td><strong><a href="/job/${jobid}/ops/REDUCE">Reduces Ops</a></strong></td>
                  <td>
                    <#if json["reduceOps"]??>
                      <#assign rops = json["reduceOps"]>
                      <#list rops?keys as kk>
                        ${kk}(${rops[kk]} rows) &nbsp;
                      </#list>
                    </#if>
                   </td>
                </tr>
                <tr>
                  <td><strong>Map Input Format</strong></td>
                  <td>
                      <#assign mipf = json["mapInputFormat"]>
                        <ul style="list-style:disc;padding-left:20px;">
                        <#list mipf as kk>
                           <li>${kk}</li>
                        </#list>
                        </ul>
                  </td>
                </tr>
                <tr>
                  <td><strong>Map Input Files</strong></td>
                  <td>
                      <#assign mip = json["mapInputs"]>
                        <ul style="list-style:disc;padding-left:20px;">
                        <#list mip as kk>
                           <li>${kk}</li>
                        </#list>
                        </ul>
                  </td>
                </tr>
                <tr style="height: 80px;">
                  <td><strong>Reduce Inputs</strong></td>
                  <td>
                      <#assign rip = json["redInputs"]>
                        <ul class="hide_list" style="list-style:disc;padding-left:20px;">
                          <#list rip as kk>
                             <li><a href="/task/${kk}">${kk}</a></li>
                          </#list>
                          <div class="show_more"> more...</div>
                          <div class="show_less"> less...</div>
                        </ul>
                  </td>
                </tr>
                <tr>
                  <td colspan="2">
                      <#assign jcounter = json["Counter"]>
                      <#list jcounter?keys as k>
                        <#if k='mapCounter'> <strong>MAP COUNTER:</strong></#if>
                        <#if k='redCounter'> <strong>REDUCE COUNTER:</strong></#if>
                        <#if k='totalCounter'> <strong>TOTAL COUNTER:</strong></#if>
                        <ul style="list-style:circle;padding-left:60px;">
                        <#assign mcounter = jcounter[k]>
                          <#list mcounter?keys as m>
                            <li>${m}: ${mcounter[m]}</li>
                          </#list>
                         </ul>
                      </#list>
                   </td>
                </tr>
            </table>
        </div>
    </div>
<#include "footer.ftl">
