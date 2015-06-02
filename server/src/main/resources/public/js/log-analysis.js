$(document).ready(function() {
  $("div#myModal .close-reveal-modal").click(function(){
    window.location.href="/";
  });
  $('.hide_list li:lt(5)').show();
  if( $('.hide_list li').size() > 5){
   $('.show_more').show();
   $('.show_less').show();
  }
  $('.show_more').click(function(){
  $('.hide_list li:gt(5)').show();
  });
  $('.show_less').click(function(){
  $('.hide_list li:gt(5)').hide();
  });
 });

