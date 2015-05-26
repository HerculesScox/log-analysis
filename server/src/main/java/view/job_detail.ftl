<#include "header.ftl">
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        <div class="table-responsive">
            <table  class="table  table-hover  table-striped table-bordered">
                <tbody>
                <tr>
                  <td  width="15%"><strong>JOB ID</strong></td>
                  <td>${jobid}</td>
                </tr>
                <tr>
                  <td><strong>Stage</strong></td>
                  <td>${stage}</td>
                </tr>
                <#assign json = detail?eval>
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
                  <td><strong>Map Ops</strong></td>
                  <td>
                    <#if json["mapOps"]??>
                      <#assign mops =json["mapOps"]>
                        <#list mops as kk>
                          ${kk}  &nbsp;
                        </#list>
                    </#if>
                   </td>
                </tr>
                <tr>
                  <td><strong>Reduces Ops</strong></td>
                  <td>
                    <#if json["reduceOps"]??>
                      <#assign rops = json["reduceOps"]>
                      <#list rops as kk>
                        ${kk}  &nbsp;
                      </#list>
                    </#if>
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