package controllers;

import javax.inject.*;
import play.*;
import play.mvc.*;
import play.libs.F;
import play.libs.ws.*;
import play.libs.Json;
import play.inject.ApplicationLifecycle;
import static play.mvc.Http.MultipartFormData.*;

import akka.actor.*;

import java.util.*;
import java.util.concurrent.*;
import scala.concurrent.duration.Duration;
import scala.concurrent.ExecutionContextExecutor;
import akka.actor.ActorSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import blackboard.*;
import controllers.blackboard.BlackboardSystem;

@Singleton
public class BlackboardApp extends Controller {

    class ConsoleWebSocket extends LegacyWebSocket<JsonNode> {
        final KGraph kgraph;
        ConsoleWebSocket (KGraph kgraph) {
            this.kgraph = kgraph;
        }

        public void onReady (WebSocket.In<JsonNode> in,
                             WebSocket.Out<JsonNode> out) {
            Logger.debug("WebSocket initialized for kgraph="+kgraph.getId());
        }
        
        public boolean isActor () { return true; }
        public akka.actor.Props actorProps (ActorRef out) {
            try {
                return akka.actor.Props.create
                    (WebSocketConsoleActor.class, out, kgraph, consoles);
            }
            catch (Exception ex) {
                throw new RuntimeException (ex);
            }
        }
    }

    private final Map<Long, ActorRef> consoles = new ConcurrentHashMap<>();
    private final Blackboard blackboard;
    private final KEvents events;

    @Inject
    public BlackboardApp (BlackboardSystem bbsys,
                          ApplicationLifecycle lifecycle) {
        blackboard = bbsys.blackboard;
        events = bbsys.events;
        
        events.subscribe(KGraph.class, ev -> {
                KGraph kg = (KGraph)ev.getEntity();
                ActorRef ref = consoles.get(kg.getId());
                if (ref != null) {
                    JsonNode json = toJson (ev, kg);
                    ref.tell(json, ActorRef.noSender());
                }
                Logger.debug(ev.getOper()+": graph "+kg.getId());
            });
        
        events.subscribe(KNode.class, ev -> {
                KGraph kg = (KGraph)ev.getSource();
                KNode kn = (KNode)ev.getEntity();               
                ActorRef ref = consoles.get(kg.getId());
                if (ref != null) {
                    JsonNode json = toJson (ev, kn);
                    ref.tell(json, ActorRef.noSender());
                }
                Logger.debug(ev.getOper()
                             +": kgraph:"+kg.getId()+" node:"+kn.getId()
                             +" type:"+kn.getType()+" name:"+kn.getName());
            });
        
        events.subscribe(KEdge.class, ev -> {
                KGraph kg = (KGraph)ev.getSource();
                KEdge ke = (KEdge)ev.getEntity();
                ActorRef ref = consoles.get(kg.getId());
                if (ref != null) {
                    JsonNode json = toJson (ev, ke);
                    ref.tell(json, ActorRef.noSender());
                }
                Logger.debug(ev.getOper()+": kgraph:"+kg.getId()
                             +" source:"+ke.getSource()+" target:"
                             +ke.getTarget());
            });

        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);
            });
    }

    void shutdown () {
        for (ActorRef ref : consoles.values()) {
            ref.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        consoles.clear();
    }

    JsonNode toJson (KEvent ev, KEntity ent) {
        ObjectNode json = Json.newObject();
        json.put("oper", ev.getOper().toString());      
        json.put("id", ent.getId());
        json.put("type", ent.getType());
        json.put("name", ent.getName());
        if (ent instanceof KGraph) {
            json.put("kind", "kgraph");
        }
        else if (ent instanceof KNode) {
            json.put("kind", "knode");
            json.put("kgraph", ((KGraph)ev.getSource()).getId());           
        }
        else if (ent instanceof KEdge) {
            json.put("kind", "kedge");
            json.put("source", ((KEdge)ent).getSource());
            json.put("target", ((KEdge)ent).getTarget());
            json.put("kgraph", ((KGraph)ev.getSource()).getId());
        }
        return json;
    }

    public Result index () {
        return ok (views.html.index.render());
    }

    public LegacyWebSocket<JsonNode> console (final Long id) {
        KGraph kg = blackboard.getKGraph(id);   
        if (kg != null) {
            return new ConsoleWebSocket (kg);
        }
        return WebSocket.reject(badRequest ());
    }
}
