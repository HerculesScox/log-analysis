<#include "header.ftl">
    <div class="col-sm-9 col-sm-offset-3 col-md-10 col-md-offset-2 main">
      <h2 class="sub-header">
        ${subTitle}
      </h2>
      <div class="login">
        <form action="/login/sign" method="get" class="form-horizontal ">
          <div class="form-group">
            <label for="Username" class="col-sm-4 control-label">Username</label>
            <div class="col-sm-4">
              <input type="text" class="form-control" name="username" placeholder="Username">
            </div>
          </div>
          <div class="form-group">
            <label for="inputPassword" class="col-sm-4 control-label">Password</label>
            <div class="col-sm-4">
              <input type="password" class="form-control" name="password" placeholder="Password">
            </div>
          </div>
          <div class="form-group">
            <div class="col-sm-offset-5 col-sm-10">
              <div class="checkbox">
                <label>
                  <input type="checkbox"> Remember me
                </label>
              </div>
            </div>
          </div>
          <div class="form-group">
            <div class="col-sm-offset-5 col-sm-10">
              <button type="submit" class="btn btn-default">Sign in</button>
            </div>
          </div>
        </form>
      </div>
    </div>
    <#if info == "failed"><script>alert("username or password invalid!") </script></#if>
<#include "footer.ftl">
