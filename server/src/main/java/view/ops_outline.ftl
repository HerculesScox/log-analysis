<#include "header.ftl">
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        <div class="table-responsive">
            <table  class="table  table-hover   table-bordered">
                <thead>
                <tr >
                    <th style="text-align:center;" width="8%">NO.</th>
                    <th style="text-align:center;" width="25%">TASK ID</th>
                    <th style="text-align:center;" >OPERATOR PROCESSING INFO</th>
                </tr>
                </thead>
                <#list allInfo as info>
                    <tr>
                        <td style="text-align:center;vertical-align:middle">${info['number']}</td>
                        <td style="text-align:center;vertical-align:middle">
                          <a href="/task/${info['taskid']}">${info['taskid']}</a>
                        </td>
                        <td>
                          <table class="table" style="margin-bottom:-2px;border-bottom:1px solid #ddd;">
                            <#assign operatorTree = info['operatorTree']>
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
                </#list>
            </table>
        </div>
    </div>
<#include "footer.ftl">