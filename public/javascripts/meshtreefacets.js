
const MESH = {
    trees: [
        'tr-C',
        'tr-D',
        'tr-G',
        'tr-A',
        'tr-E'
    ],
    selection: {}
};

// FIXME!!!
const MESH_SEARCH_PREFIX = '/search?';
const MESH_MAXCOUNT = 2019; // max facet count to allow 

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
    
    var params = new URLSearchParams (window.location.search);
    var facets = params.getAll('facet');
    for (var i in MESH.trees) {
        const id = MESH.trees[i];
        $('#'+id).jstree({
            'core': {
                'themes': {
                    'name': 'proton',
                    'responsive': true,
                }
            }
        }).on('select_node.jstree', function (ev, data) {
            console.log(data.node.id+' is selected');
            MESH.selection[data.node.id] = id;
            console.log(MESH.selection);
            updateTreeHeader (id);
        }).on('deselect_node.jstree', function(ev, data) {
            console.log(data.node.id+' it deselected');
            delete MESH.selection[data.node.id];
            console.log(MESH.selection);
            updateTreeHeader (id);
        }).on('deselect_all.jstree', function () {
            updateTreeHeader (id);
            MESH.selection = {};
        }).on('ready.jstree click', function (e, data) {
            // remove all non-custom icon
            $('i.jstree-themeicon').not('.jstree-themeicon-custom').remove();
            $('.pubmed-facetnode').each(function (index) {
                var count = $(this).attr('data-facetcount');
                console.log($(this).attr('id')+' '+count);
                if (count > MESH_MAXCOUNT) {
                    $.jstree.reference('#'+id).disable_node($(this));
                }
            });
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
    var params = new URLSearchParams (window.location.search);
    var facets = params.getAll('facet');
    var pruned = [];
    for (var i in facets) {
        var pos = facets[i].indexOf('/');
        var node = facets[i].substring(pos+1);
        console.log(facets[i]+'...'+tr.is_selected(node));
        if (!tr.is_selected(node))
            pruned.push(facets[i]);
    }
    params.delete('facet');
    for (var i in pruned) {
        params.append('facet', pruned[i]);
    }
    params.delete('skip');
    tr.deselect_all();
    applySearch (params);
}

function applyTreeSelections (el) {
    var paths = [];
    for (var p in MESH.selection) {
        paths.push(p);
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
    
    applySearch (params);
}

function applySearch (params) {
    //var url = window.location.pathname+'?'+params.toString();
    var url = MESH_SEARCH_PREFIX+params.toString(); // FIXME!!!!!
    console.log('=====> '+url);
    $.ajax({
        'url': url,
        success: function () {
            window.location.href = url;
        }
    }).done(function () {
        
    });              
}
