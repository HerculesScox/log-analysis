<#include "header.ftl">
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        <div class="table-responsive">
            <table  class="table  table-hover  table-striped table-bordered">
                <thead>
                <tr >
                    <th style="text-align:center;" width="8%">NO.</th>
                    <th style="text-align:center;" width="35%">TASK ID</th>
                    <th style="text-align:center;" width="20%">TASK TYPE</th>
                    <th style="text-align:center;" >DETAIL INFO</th>
                </tr>
                </thead>
                <tbody>

                <#list allInfo as info>
                    <tr>
                        <td style="text-align:center;vertical-align:middle">${info['number']}</td>
                        <td style="text-align:center;vertical-align:middle">${info['taskid']}</td>
                        <td style="text-align:center;vertical-align:middle">${info['type']}</td>
                        <td style="text-align:center;vertical-align:middle">
                          <a href="/task/${info['taskid']}">detail information</a>
                        </td>
                    </tr>
                </#list>
            </table>
        </div>
        <nav style="text-align:center">
          <ul class="pagination" style="padding-left:0px;margin-left:auto;margin-right:auto;">
            <li>
              <a href="#" aria-label="Previous">
                <span aria-hidden="true">&laquo;</span>
              </a>
            </li>
            <li><a href="#">1</a></li>
            <li><a href="#">2</a></li>
            <li><a href="#">3</a></li>
            <li><a href="#">4</a></li>
            <li><a href="#">5</a></li>
            <li>
              <a href="#" aria-label="Next">
                <span aria-hidden="true">&raquo;</span>
              </a>
            </li>
          </ul>
        </nav>
    </div>
<#include "footer.ftl">