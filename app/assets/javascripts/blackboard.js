
function websocket (id, url) {
    var ws = new WebSocket(url);
    ws.onmessage = function(ev) {
	//var data = JSON.parse(ev.data);
	console.log(ev.data);
    };
}
