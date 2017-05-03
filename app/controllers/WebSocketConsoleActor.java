package controllers;

import java.util.*;
import akka.actor.*;
import play.libs.Json;
import play.Logger;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;

import blackboard.*;
import static blackboard.KEntity.*;


public class WebSocketConsoleActor extends UntypedActor {
    final ActorRef out;
    final KGraph kgraph;
    final Map<Long, ActorRef> consoles;
    
    public WebSocketConsoleActor (ActorRef out, KGraph kgraph,
                                  Map<Long, ActorRef> consoles) {
        this.out = out;
        this.kgraph = kgraph;
        this.consoles = consoles;
        Logger.debug("WebSocketConsoleActor created "+self().path()
                     +" for kgraph="+kgraph.getId()+" "+out);
        
        consoles.put(kgraph.getId(), self ());
    }
    
    public void onReceive (Object message) throws Exception {
        if (message instanceof JsonNode) {
            out.tell(message, self ()); // pass thru socket
        }
        else {
            unhandled (message);
        }
    }
    
    @Override
    public void postStop () {
        Logger.debug("Closing console kgraph "+kgraph.getId());
        consoles.remove(kgraph.getId());
    }
}
