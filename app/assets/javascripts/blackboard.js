
var cy;

function removeNode (id) {
    console.log('remove node '+id);
    cy.$('#'+id).removeClass('toggle');
    $('#panel-n'+id).remove();
}

function nodehtml (nid, n) {
    var html = '<div id="'+nid+'" class="panel panel-default">'+
        '<div class="panel-heading">'+
        '<h1 class="panel-title">';
    var uri = n.data('uri');
    if (typeof uri != 'undefined') {
        html += '<a href="'+uri+'">'+n.data('name')+'</a>';
    }
    else {
        html += n.data('name');
    }
    html += '<button type="button" onclick="removeNode('+n.id()+')" class="close"><span aria-hidden="true">&times;</span></button></h1>'+
    '</div>'+
    '<div class="panel-body">'+
    ' <div class="table-responsive">'+
    '   <table class="table table-condensed">';
    for (var f in n.data()) {
        html += '   <tr><td><b>'+f+'</b></td><td>';
        var v = ''+n.data(f);
        if (f === 'uri' || v.startsWith('http://') ||v.startsWith('https://')) {
            html += '<a href="'+v+'" target="'+f+'">'+v+'</a>';
        }
        else {
            html += v;
        }
        html += '</td></tr>';
    }
    html += ' </table></div></div></div>';
    return html;
}

function renderKGraph (id, url, wsurl) {
    $.get(url, function (kdata) {
        cy = kgraph (id);
        cy.on('tap', 'node', function (evt) {
            var node = evt.target;
            node.toggleClass('toggle');
            var nid = 'panel-n'+node.id();
            if (node.hasClass('toggle')) {
                if ($('#'+nid).length === 0) {
                    $('#selection-panel').prepend(nodehtml (nid, node));
                }
            }
            else {
                $('#'+nid).remove();
            }
        });
        
        var ws = new WebSocket (wsurl);
        ws.onmessage = function (ev) {
            var data = JSON.parse(ev.data);
            //console.log(data);
            data.id = data['@id'];
            data.type = data['@type'];
            switch (data.oper) {
            case "ADD":
                switch (data.kind) {
                case "knode":
                    var n = cy.add({
                        group: "nodes",
                        data: data
                    });
                    break;
                    
                case "kedge":
                    if (data.source != data.target) {
                        var e = cy.add({
                            group: "edges",
                            data: data
                        });
                    }
                    break;
                    
                default:
                    console.log('Unknown node kind: '+data.kind);
                }
                break;
                
            default:
                console.log('Unknown operation: "'+data.oper+'"');
            }
            var layout = cy.layout({
                name: 'cose'
            });
            layout.run();
        };
        
        var i = 0, len = kdata.nodes.length;
        for (; i < len; ++i) {
            var kn = kdata.nodes[i];
            kn.id = kn['@id'];
            kn.type = kn['@type'];
            //console.log('node added: '+kn);
            var n = cy.add({
                group: "nodes",
                data: kn
            });
        }
        
        len = kdata.edges.length;   
        for (i = 0; i < len; ++i) {
            var e = kdata.edges[i];
            if (e.source != e.target) {
                e.type = e['@type'];
                cy.add({
                    group: "edges",
                    data: e
                });
            }
        }
        
        var layout = cy.layout({
            name: 'cose'
        });
        layout.run();
    });
}

function kgraph (id) {
    return cytoscape({
        container: $(id), // container to render in
        style: [ // the stylesheet for the graph
            {
                selector: 'node',
                style: {
                    'background-color': '#666',
                    'label': 'data(name)',
                    'text-wrap': 'ellipsis',
                    'text-max-width': 400
                }
            },
            {
                selector: "node[type='concept']",
                style: {
                    'background-color': '#b780f2'
                }
            },
            {
                selector: "node[type='drug']",
                style: {
                    'background-color': '#0f5'
                }
            },
            {
                selector: "node[type='article']",
                style: {
                    'shape': 'roundrectangle',
                    'background-color': '#ff8000',
                    'font-weight': 'bold',
                    'color': '#ff8000'
                }
            },
            {
                selector: "node[type='protein']",
                style: {
                    'background-color': 'blue'
                }
            },
            {
                selector: "node[type='adverse'],[type='disease']",
                style: {
                    'background-color': '#f20'
                }
            },
            {
                selector: "node[type='query']",
                style: {
                    'background-color': '#f5e',
                    'width': 50,
                    'height': 50
                }
            },
            {
                selector: '.toggle',
                style: {
                    'border-width': '5%',
                    'border-color': '#aaa',
                    'padding': 5
                }
            },
            {
                selector: 'edge',
                style: {
                    'width': 2,
                    'label': 'data(type)',
                    'line-color': '#ccc',
                    'target-arrow-color': '#ccc',
                    'target-arrow-shape': 'triangle'
                }
            }
        ]
    });
}
