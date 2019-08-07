package blackboard.disease;

import java.util.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.concurrent.*;
import static java.util.concurrent.CompletableFuture.supplyAsync;

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
import play.cache.AsyncCacheApi;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;
import akka.pattern.Patterns;
import static akka.dispatch.Futures.sequence;
import static scala.compat.java8.FutureConverters.*;
import akka.util.Timeout;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.SerializationFeature;

import blackboard.utils.Util;

public class DiseaseKSource implements KSource, KType {
    static final Timeout TIMEOUT = new Timeout (60, TimeUnit.SECONDS);

    static Props props (DiseaseKSource dks) {
        return Props.create(DiseaseActor.class, () -> new DiseaseActor (dks));
    }
    
    static class DiseaseActor extends AbstractActor {
        final DiseaseKSource dks;
        public DiseaseActor (DiseaseKSource dks) {
            this.dks = dks;
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
                .match(String.class, this::doResolve)
                .match(Long[].class, this::fetch)
                .match(Long.class, this::fetch)
                .match(Object.class, this::unknown)
                .build();
        }

        void doQuery (DiseaseQuery q) {
            Logger.debug(self()+": query="+q.query);
            DiseaseResult result = new DiseaseResult (q);
            try {
                long start = System.currentTimeMillis();
                WSRequest req = dks.wsclient.url
                    (dks.api+"/search/"+q.getQuery())
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

        void fetch (Long... ids) {
            Logger.debug(self()+": ids="+ids);
            long start = System.currentTimeMillis();
            
            List<Disease> diseases = new ArrayList<>();
            List<CompletableFuture> futures = new ArrayList<>();
            for (Long id : ids) {
                WSRequest req = dks.wsclient.url(dks.api+"/entity/"+id);
                CompletableFuture f  = req.get().thenAcceptAsync(res -> {
                        Logger.debug(self()+": fetching node "+id
                                     +" ..."+res.getStatus());
                        if (200 == res.getStatus()) {
                            JsonNode json = res.asJson();
                            Disease d = parseDisease (json);
                            if (d != null) {
                                diseases.add(d);
                                Logger.debug("## "+d.id+": "+d.name);
                            }
                            else
                                Logger.warn("Node "+id
                                            +" is not a valid disease!");
                        }
                    }, dks.dec).toCompletableFuture();
                futures.add(f);
            }
            
            CompletableFuture.allOf
                (futures.toArray(new CompletableFuture[0])).join();
            getSender().tell(diseases, getSelf ());
            
            Logger.debug(self()+": fetch executed in "+String.format
                         ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start)));
        }

        // find the best matching entry for a given name
        void doResolve (String name) {
            Logger.debug(self()+": resolve="+name);
            long start = System.currentTimeMillis();
            
            Disease disease = Disease.NONE;
            int skip = 0, top = 20, total = 0;
            do {
                try {
                    WSRequest req = dks.wsclient.url
                        (dks.api+"/search/"+name)
                        .setQueryParameter("skip", String.valueOf(skip))
                        .setQueryParameter("top", String.valueOf(top));
                    WSResponse res = req.get().toCompletableFuture().get();
                    Logger.debug(self()+"  ++++ "+req.getUrl()
                                 +"..."+res.getStatus());
                    if (200 == res.getStatus()) {
                        JsonNode json = res.asJson();
                        if (total == 0) {
                            total = json.get("total").asInt();
                            Logger.debug(self()+"  ++++ "+req.getUrl()
                                         +"..."+total);
                        }

                        disease = resolveDisease (name, json);
                        if (disease != Disease.NONE)
                            break;
                        skip += json.get("count").asInt();
                    }
                    else {
                        Logger.warn(getSelf()+": "+req.getUrl()
                                    +" status="+res.getStatus());
                        break;
                    }
                }
                catch (Exception ex) {
                    Logger.error("Can't execute search", ex);
                    break;
                }
            }
            while (skip < total);
            
            if (disease == Disease.NONE)
                Logger.warn("** Can't resolve \""+name+"\" to a disease!");
            
            getSender().tell(disease, getSelf ());
        }

        void unknown (Object obj) {
            Logger.warn(getSelf()+": unknown message: "+obj);
        }
        
        void parseDiseaseResult (DiseaseResult result, JsonNode json) {
            Logger.debug("####### q="+json.get("query").asText()
                         +" total="+json.get("total").asInt());
            result.total = json.get("total").asInt();
            JsonNode contents = json.get("contents");
            if (contents != null) {
                for (int i = 0; i < contents.size(); ++i) {
                    Disease d = parseDisease (contents.get(i));
                    if (d != null) {
                        result.add(d);
                    }
                    else {
                        --result.total;
                    }
                }
            }
        }

        Disease resolveDisease (String name, JsonNode json) {
            Disease disease = Disease.NONE;
            JsonNode contents = json.get("contents");
            if (contents != null && contents.isArray()) {
                for (int i = 0; i < contents.size()
                         && disease == Disease.NONE; ++i) {
                    json = contents.get(i);
                    JsonNode payload = json.at("/payload/0");
                    Logger.debug(i+": "+payload);
                    for (String idf : Disease.ID_FIELDS) {
                        if (payload.has(idf)) {
                            JsonNode node = payload.get(idf);
                            if (node.asText().indexOf(name) >= 0) {
                                try {
                                    disease = Disease.getInstance(json);
                                }
                                catch (Exception ex) {
                                    Logger.warn("Not a valid disease json: "
                                                +json);
                                }
                            }
                            break;
                        }
                    }
                }
            }
            return disease;
        }

        Disease parseDisease (JsonNode json) {
            try {
                return Disease.getInstance(json);
            }
            catch (Exception ex) {
                Logger.error("Can't parse disease: "+json, ex);
            }
            return null;
        }
    } // DiseaseActor
    
    final AsyncCacheApi cache;
    final WSClient wsclient;
    final ActorSystem actorSystem;
    final String api;
    final ActorRef apiActor;
    final KSourceProvider ksp;
    final DiseaseExecutionContext dec;
    
    @Inject
    public DiseaseKSource (WSClient wsclient, AsyncCacheApi cache,
                           @Named("disease") KSourceProvider ksp,
                           ActorSystem actorSystem,
                           DiseaseExecutionContext dec,
                           ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.cache = cache;
        this.actorSystem = actorSystem;
        this.ksp = ksp;
        this.dec = dec;

        Map<String, String> props = ksp.getProperties();
        api = props.get("api");
        if (api == null)
            throw new IllegalArgumentException
                (ksp.getId()+": No api property defined for diseases!");
        apiActor = actorSystem.actorOf(props (this));
            
        lifecycle.addStopHook(() -> {
                wsclient.close();
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    protected void instrument (Disease d) {
        List<CompletableFuture> futures = new ArrayList<>();
        
        JsonNode nodes = d.node.at("/parents");
        if (nodes.size() > 0) {
            CompletableFuture f =
                fetchDiseases(nodes).thenApplyAsync
                (parents -> d.parents.addAll(parents), dec)
                .toCompletableFuture();
            futures.add(f);
        }

        /*
        nodes = d.node.at("/children");
        if (nodes.size() > 0) {
            CompletableFuture f =
                fetchDiseases(nodes).thenApplyAsync
                (children -> d.children.addAll(children), dec)
                .toCompletableFuture();
            futures.add(f);
        }
        */
        
        if (!futures.isEmpty()) {
            CompletableFuture.allOf
                (futures.toArray(new CompletableFuture[0])).join();
        }
    }
    
    CompletionStage<List<Disease>> fetchDiseases (Long... ids) {
        final String key = Util.sha1(ids);
        return cache.getOrElseUpdate(key, () -> {
                CompletableFuture future = toJava
                    (Patterns.ask(apiActor, ids, TIMEOUT))
                    .toCompletableFuture();
                return future.thenApplyAsync
                    (result -> ((List)result).stream().map(d -> (Disease)d)
                     .collect(Collectors.toList()));
            });
    }
    
    CompletionStage<List<Disease>> fetchDiseases (JsonNode nodes) {
        Long[] ids = new Long[nodes.size()];
        for (int i = 0; i < ids.length; ++i) {
            ids[i] = nodes.get(i).asLong();
        }
        return fetchDiseases (ids);
    }
    
    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
    }

    public CompletionStage<DiseaseResult> _search
        (String q, int skip, int top) {
        DiseaseQuery dq = new DiseaseQuery (q, skip, top);
        try {
            CompletableFuture future = toJava
                (Patterns.ask(apiActor, dq, TIMEOUT)).toCompletableFuture();

            return future.thenApplyAsync(result -> (DiseaseResult)result, dec);
        }
        catch (Exception ex) {
            Logger.error("Unable to process query "
                         +"with disease api: "+q);
        }
        
        return supplyAsync (() -> DiseaseResult.EMPTY, dec);
    }
    
    public CompletionStage<DiseaseResult> search
        (final String q, final int skip, final int top) {
        return cache.getOrElseUpdate
            (Util.sha1(q)+"/"+skip+"/"+top, () -> _search (q, skip, top));
    }

    public CompletionStage<Disease> getDisease (Long id) {
        CompletableFuture future = toJava
            (Patterns.ask(apiActor, id, TIMEOUT)).toCompletableFuture();
        
        return future.thenApplyAsync
            (result -> ((List)result).isEmpty()
             ? null : (Disease)((List)result).get(0), dec)
            .thenApplyAsync(v -> {
                    Disease d = (Disease)v;
                    instrument (d);
                    return d;
                }, dec);
    }

    public CompletionStage<Disease> disease (Long id) {
        final String key = getClass().getName()+"/"+id;
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed... "+key);
                return getDisease (id);
            });
    }

    public CompletionStage<Disease> resolve (String id) {
        final String key = getClass().getName()+"/"+id;
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed... "+key);
                CompletableFuture future = toJava
                    (Patterns.ask(apiActor, id, TIMEOUT))
                    .toCompletableFuture();
                return future.thenApplyAsync(v -> {
                        Disease d = (Disease)v;
                        instrument (d);
                        return d;
                    }, dec);
            });
    }
}
