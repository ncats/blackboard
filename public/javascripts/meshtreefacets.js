
function setupTreeFacets () {
    $.jstree.defaults.core.animation = 0;
    $.jstree.defaults.core.themes.icons = false;
    $.jstree.defaults.core.themes.stripes = false;  
    $.jstree.defaults.core.themes.responsive = true;
    $.jstree.defaults.checkbox.cascade = "up";
    //$.jstree.defaults.checkbox.whole_node = false;
    $.jstree.defaults.plugins = ["checkbox"];
    $.jstree.defaults.dnd.drag_selection = true;
    $.jstree.defaults.dnd.large_drop_target = true;
    const trees = [
        'tr-C',
        'tr-D',
        'tr-G',
        'tr-A',
        'tr-E'
    ];
    
    var params = new URLSearchParams (window.location.search);
    var facets = params.getAll('facet');
    for (var i in trees) {
        const id = trees[i];
        $('#'+id).jstree({
            'core': {
                'themes': {
                    'name': 'proton',
                    'responsive': true
                }
            }
        }).on('select_node.jstree', function (ev, data) {
            console.log(data.node.id+' is selected');
            console.log(data.selected);
            updateTreeHeader (id);
        }).on('deselect_node.jstree', function(ev, data) {
            console.log(data.node.id+' it deselected');
            console.log(data.selected);
            updateTreeHeader (id);
        }).on('deselect_all.jstree', function () {
            updateTreeHeader (id);
        });
    }
    
    var trcnt = 0;
    for (var i in facets) {
        var f = facets[i];
        if (f.startsWith('@tr')) {
            var nid = f.substring(f.indexOf('/')+1);
            var id = '#tr-'+nid.substring(0, 1);
            var tr = $.jstree.reference(id);
            tr.select_node('#'+nid);
            ++trcnt;
        }
    }
    
    if (trcnt > 0) {
        $('#menu-toggle').html("Less...");
        $('#wrapper').toggleClass('toggled');
    }
}

function updateTreeHeader (tree) {
    var id = '#header-'+tree;
    var name = $(id).data('name');
    var selected = $.jstree.reference('#'+tree).get_top_selected();
    if (selected.length == 0) {
        $(id).html(name);
    }
    else {
        $(id).html(name+' ('+selected.length+')');
    }
}

function clearTreeSelections (el) {
    var id = $(el).data("treeid");
    var tr = $.jstree.reference('#'+id);
    var selected = tr.get_bottom_selected();
    var params = new URLSearchParams (window.location.search);
    var facets = params.getAll('facet');
    for (var i in facets) {
        var node = $.jstree.reference('#'+facets[i]);
        console.log(facets[i]+'...'+tr.is_selected(node));   
    }
    tr.deselect_all();
}

function applyTreeSelections (el) {
    const trees = [
        'tr-C',
        'tr-D',
        'tr-G',
        'tr-A',
        'tr-E'
    ];
    var paths = [];
    for (var i in trees) {
        var tr = $.jstree.reference('#'+trees[i]);
        if (tr != null) {
            var selected = tr.get_top_selected();
            console.log('#'+trees[i]+'...');
            for (var n in selected) {
                console.log('...'+selected[n]);
                paths.push(selected[n]);
            }
        }
    }
    
    console.log('selected paths...'+paths);
    var params = new URLSearchParams (window.location.search);
    var facets = params.getAll('facet');
    params.delete('facet');
    for (var i in facets) {
        var f = facets[i];
        if (f.startsWith('@tr')) {
            
        }
        else {
            params.append('facet', f);
            console.log('keeping...'+f);
        }
    }
    
    if (paths.length > 0) {
        for (var i in paths) {
            var fname = '@tr'+paths[i].substring(0,1)+'/';
            params.append('facet', fname+paths[i]);
        }
        params.delete('skip');
    }
    
    //var url = window.location.pathname+'?'+params.toString();
    var url = '/search?'+params.toString(); // FIXME!!!!!
    console.log('=====> '+url);
    $.ajax({
        'url': url,
        success: function () {
            window.location.href = url;
        }
    }).done(function () {
        
    });              
}
