
/*
// must already have  <script src="@_root_.blackboard.pubmed.controllers.routes.Controller.jsRoutes" type="text/javascript"></script>  
*/
function get_pubmed_count(id, query) {
    var route = pubmedRoutes.blackboard.pubmed
        .controllers.Controller.search(query);
    var url = route.url;
    url += (url.indexOf('?') > 0 ? '&':'?')+'path=/total';
    console.log('***** '+query+' => '+route.url);
    $.ajax({
        url: url,
        success: function(data) {
            console.log('***** '+url+' '+data);
            $(id).html(''+data);
        },
        error: function(){
            $(id).html('?');
            console.log('failed to retrieve publication count!');
        }
    });
}
