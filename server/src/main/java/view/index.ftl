<#include "header.ftl">
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
        <h2 class="sub-header">${subTitle}</h2>
        <div class="table-responsive">
            <table class="table  table-hover  table-striped">
                <thead>
                <tr>
                    <th>NO.</th>
                    <th>USERNAME</th>
                    <th>QUERY</th>
                    <th>JOB DEPENDENCY</th>
                    <th>JOB AMOUNT</th>
                </tr>
                </thead>
                <tbody>

                <#list allInfo as info>
                    <tr>
                        <td>${info["number"]}</td>
                        <td>${info["username"]!}</td>
                        <td>${info["queryString"]}</td>
                        <td>${info["jobDependency"]}</td>
                        <td>${info["jobAmount"]}</td>
                    </tr>
                </#list>

                <tr>
                    <td>1,013</td>
                    <td>torquent</td>
                    <td>per</td>

                    <td>conubia</td>
                    <td>nostra</td>
                </tr>
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