
const FACET = {
    trees: [
        'tr-C',
        'tr-D',
        'tr-G',
        'tr-A',
        'tr-E',
        'grantagency'
    ],
    selection: {}
};

const FACET_MAXCOUNT = 2019; // max facet count to allow 

function setupTreeFacets () {
    $.jstree.defaults.core.animation = 0;
    $.jstree.defaults.core.themes.icons = false;
    $.jstree.defaults.core.themes.stripes = false;  
    $.jstree.defaults.core.themes.responsive = true;
    //$.jstree.defaults.checkbox.cascade = "undetermined";
    //$.jstree.defaults.checkbox.whole_node = false;
    $.jstree.defaults.plugins = ["checkbox"];
    $.jstree.defaults.dnd.drag_selection = true;
    $.jstree.defaults.dnd.large_drop_target = true;
    
    const params = new URLSearchParams (window.location.search);
    const facets = params.getAll('facet');
    for (var i in FACET.trees) {
        const id = FACET.trees[i];
        $('#'+id).jstree({
            'core': {
                'themes': {
                    'name': 'proton',
                    'responsive': true,
                }
            }
        }).on('select_node.jstree', function (ev, data) {
            console.log(data.node.id+' is selected');
            FACET.selection[data.node.id] = id;
            console.log(FACET.selection);
            updateTreeHeader (id);
        }).on('deselect_node.jstree', function(ev, data) {
            console.log(data.node.id+' it deselected');
            delete FACET.selection[data.node.id];
            console.log(FACET.selection);
            updateTreeHeader (id);
        }).on('deselect_all.jstree', function () {
            updateTreeHeader (id);
            FACET.selection = {};
        }).on('ready.jstree click', function (e, data) {
            // remove all non-custom icon
            $('i.jstree-themeicon').not('.jstree-themeicon-custom').remove();
            $('.pubmed-facetnode').each(function (index) {
                var count = $(this).attr('data-facetcount');
                console.log($(this).attr('id')+' '+count);
                if (count > FACET_MAXCOUNT) {
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
            if (tr != null) {
                tr.select_node('#'+nid);
                ++trcnt;
            }
        }
        else if (f.startsWith('@grantagency')) {
            var nid = f.substring(f.indexOf('/')+1);
            var tr = $.jstree.reference('#grantagency');
            if (tr != null)
                tr.select_node('#'+nid);            
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
    const id = $(el).data("treeid");
    const tr = $.jstree.reference('#'+id);
    const params = new URLSearchParams (window.location.search);
    const facets = params.getAll('facet');
    const pruned = [];
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
    const params = new URLSearchParams (window.location.search);
    const facets = params.getAll('facet');
    params.delete('facet');
    for (var i in facets) {
        var f = facets[i];
        if (f.startsWith('@tr') || f.startsWith('@grantagency')) {
            
        }
        else {
            params.append('facet', f);
            console.log('keeping...'+f);
        }
    }
    
    for (var p in FACET.selection) {
        var f = FACET.selection[p];
        if (f.startsWith('tr')) {
            var fname = '@tr'+p.substring(0,1)+'/';
            params.append('facet', fname+p);
        }
        else if (f == 'grantagency') {
            params.append('facet', '@grantagency/'+p);
        }
    }
    params.delete('skip');    
    applySearch (params);
}

function showloader () {
    $('#content-panel').each(function (index) {
        $(this).addClass('is-active');
    });
}

function hideloader () {
    $('#content-panel').removeClass('is-active');
}

function applySearch (params) {
    const url = window.location.pathname+'?'+params.toString();
    console.log('=====> '+url);
    
    showloader ();
    $.ajax({
        'url': url,
        success: function () {
            window.location.href = url;
        }
    }).done(function () {
        $('.loader').each(function (index) {
            $(this).removeClass('is-active');
        });
    });
}
