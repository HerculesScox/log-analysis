<#include "header.ftl">
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        <div class="table-responsive">
            <table  class="table  table-hover  table-striped table-bordered">
                <thead>
                <tr >
                    <th style="text-align:center;" width="8%">NO.</th>
                    <th style="text-align:center;" width="25%">JOB ID</th>
                    <th style="text-align:center;" width="10%">STAGE</th>
                    <th style="text-align:center;" width="25%">LAUNCH TIME</th>
                    <th style="text-align:center;" width="10%">TASKS</th>
                    <th style="text-align:center;" >DETAIL INFO</th>
                </tr>
                </thead>
                <tbody>

                <#list allInfo as info>
                    <tr>
                        <td style="text-align:center;vertical-align:middle">${info['number']}</td>
                        <td style="text-align:center;vertical-align:middle">${info['jobid']}</td>
                        <td style="text-align:center;vertical-align:middle">${info['stage']}</td>
                        <td style="text-align:center;vertical-align:middle">${info['launchtime']?number_to_datetime}</td>
                        <td style="text-align:center;vertical-align:middle">
                         <a href="/tasks/${info['jobid']}">${info['taskNum']}</a>
                        </td>
                        <td style="text-align:center;vertical-align:middle">
                          <a href="/job/${info['jobid']}">detail information</a>
                        </td>
                    </tr>
                </#list>
            </table>
        </div>
    </div>
<#include "footer.ftl">