package blackboard.disease;

import java.util.*;
import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Callable;

import blackboard.*;
import static blackboard.KEntity.*;

import play.Configuration;
import play.mvc.*;
import play.libs.ws.*;
import play.Logger;
import play.Environment;
import play.mvc.BodyParser;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;
import play.inject.ApplicationLifecycle;
import play.cache.SyncCacheApi;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.SerializationFeature;

import blackboard.utils.Util;

public class DiseaseKSource implements KSource, KType {
    static class DiseaseActor extends AbstractActor {
        static Props props (WSClient wsclient, String url) {
            return Props.create
                (DiseaseActor.class, () -> new DiseaseActor (wsclient, url));
        }

        final WSClient wsclient;
        final String url;
        public DiseaseActor (WSClient wsclient, String url) {
            this.wsclient = wsclient;
            this.url = url;
        }

        @Override
        public void preStart () {
            Logger.debug("### "+self ()+ "...initialized!");
        }

        @Override
        public void postStop () {
            Logger.debug("### "+self ()+"...stopped!");
        }

        @Override
        public Receive createReceive () {
            return receiveBuilder()
                .match(DiseaseQuery.class, this::doQuery)
                .build();
        }

        void doQuery (DiseaseQuery q) {
            Logger.debug(self()+": query="+q.query);
            DiseaseResult result = new DiseaseResult (q);
            try {
                long start = System.currentTimeMillis();
                WSRequest req = wsclient.url(url+q.getQuery())
                    .setQueryParameter("skip", String.valueOf(q.skip))
                    .setQueryParameter("top", String.valueOf(q.top));
                WSResponse res = req.get().toCompletableFuture().get();
                Logger.debug(self()+"  ++++ "+req.getUrl()
                             +"..."+res.getStatus());
                if (200 == res.getStatus()) {
                    JsonNode json = res.asJson();
                    parseDiseaseResult (result, json);
                }
                else {
                    result.setStatus(res.getStatus(), req.getUrl());
                }
                Logger.debug(self()+": search executed in "+String.format
                             ("%1$.3fs",
                              1e-3*(System.currentTimeMillis()-start)));
            }
            catch (Exception ex) {
                Logger.error(self()+": Can't execute search api", ex);
                result.setStatus(-1, ex.getMessage());
            }
            getSender().tell(result, getSelf ());
        }
    } // DiseaseActor


    static void parseDiseaseResult (DiseaseResult result, JsonNode json) {
        Logger.debug("####### q="+json.get("query").asText()
                     +" total="+json.get("total").asInt());
        result.total = json.get("total").asInt();
        JsonNode contents = json.get("contents");
        if (contents != null) {
            for (int i = 0; i < contents.size(); ++i) {
                Disease d = Disease.getInstance(contents.get(i));
                result.add(d);
            }
        }
    }
    

    final ObjectMapper mapper = Json.mapper();
    final SyncCacheApi cache;
    final WSClient wsclient;
    final ActorSystem actorSystem;
    final String api;
    final ActorRef diseaseActor;
    final KSourceProvider ksp;
    
    @Inject
    public DiseaseKSource (WSClient wsclient, SyncCacheApi cache,
                           @Named("disease") KSourceProvider ksp,
                           ActorSystem actorSystem,
                           ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.cache = cache;
        this.actorSystem = actorSystem;
        this.ksp = ksp;

        Map<String, String> props = ksp.getProperties();
        api = props.get("api");
        if (api == null)
            throw new IllegalArgumentException
                (ksp.getId()+": No api property defined for diseases!");
        diseaseActor = actorSystem.actorOf(DiseaseActor.props(wsclient, api));
            
        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        lifecycle.addStopHook(() -> {
                wsclient.close();
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }
    
    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
    }

    public DiseaseResult search (String q, int skip, int top) {
        return cache.getOrElseUpdate
            (Util.sha1(q)+"/"+skip+"/"+top, new Callable<DiseaseResult>() {
                    public DiseaseResult call () throws Exception {
                        DiseaseQuery dq = new DiseaseQuery (q, skip, top);
                        Inbox inbox = Inbox.create(actorSystem);
                        inbox.send(diseaseActor, dq);
                        try {
                            DiseaseResult result = (DiseaseResult)
                                inbox.receive(Duration.ofSeconds(5));
                            return result;
                        }
                        catch (TimeoutException ex) {
                            Logger.error("Unable to process query "
                                         +"with disease api: "+q);
                        }
                        
                        return DiseaseResult.EMPTY;
                    }
                });
    }
}
