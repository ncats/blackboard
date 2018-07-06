package blackboard.beacons;

import java.net.URL;
import java.util.*;
import java.util.function.BiConsumer;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import akka.actor.ActorSystem;
import play.Logger;
import play.libs.ws.*;
import play.libs.Json;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import com.fasterxml.jackson.databind.JsonNode;

import blackboard.*;
import static blackboard.KEntity.*;

public class BeaconKSource implements KSource {

    interface Resolver {
        void resolve (JsonNode json, KNode kn, KGraph kg);
    }
    private static final Map<String, String> SEMANTIC_GROUPS;
    static
    {
        SEMANTIC_GROUPS = new HashMap<String, String>();
        SEMANTIC_GROUPS.put("CHEM", "drug");
        SEMANTIC_GROUPS.put("DISO", "disease");
        SEMANTIC_GROUPS.put("GENE","gene");
    }

    protected final KSourceProvider ksp;    
    @Inject protected WSClient wsclient;
    
    @Inject
    public BeaconKSource (ActorSystem actorSystem, WSClient wsclient,
                          @Named("beacons") KSourceProvider ksp,
                          ApplicationLifecycle lifecycle) {
        this.ksp = ksp;
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    @Inject
    protected void setLifecycle (ApplicationLifecycle lifecycle) {
        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        Logger.debug("lifecycle hook registered!");
    }
    
    protected void seedQuery (String term, KNode kn, KGraph kg) {
        String url = ksp.getUri()+"/concepts";
        try {
            Map<String, String> q = new HashMap<>();
            q.put("keywords",term);
            resolve(ksp.getUri()+"/concepts",q,kn,kg,this::resolveQuery);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }

//    void instrument (String entity, JsonNode json, KNode kn, KGraph kg,
//                     BiConsumer<JsonNode, Map<String, Object>> consumer) {
//        String uri = null;
//        if (json.hasNonNull("uri"))
//            uri = json.get("uri").asText();
//        if(json.get("id")!=null)
//        {
//            String id = json.get("id").asText();
//            String name = json.get("name").asText();
//            Map<String, Object> props = new TreeMap<>();
//            props.put(URI_P, ksp.getUri()+"/concepts/"+id);
//            props.put(NAME_P, name);
//            consumer.accept(json, props);
//
//            KNode node = kg.createNodeIfAbsent(props, URI_P);
//            if (node.getId() != kn.getId()) {
//                node.addTag("KS:"+ksp.getId());
//                props.clear();
//                props.put("value", uri);
//                kg.createEdgeIfAbsent(kn, node, "resolve");
//                Logger.debug(node.getId()+"..."+name);
//            }
//        }
//
//    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        if (nodes == null || nodes.length == 0)
            nodes = kgraph.getNodes();

        for (KNode kn : nodes) {
            if(kn.get("term")!=null)
            {
                seedConcept ((String)kn.get("term"), kn, kgraph);
            }
            else
            {
                seedConcept((String)kn.get("name"),kn,kgraph);
            }
//            if (kn.get("clique")!=null){
//                String clique = kn.get("clique").toString();
//                seedConcept(clique,kn,kgraph);
//            }

        }
    }
    void resolve (String url, Map<String, String> params,
                  KNode kn, KGraph kg, Resolver resolver) {
        WSRequest req = wsclient.url(url).setFollowRedirects(true);

        if (params != null) {
            for (Map.Entry<String, String> me : params.entrySet()) {
                Logger.debug(".."+me.getKey()+": "+me.getValue());
                req = req.setQueryParameter(me.getKey(), me.getValue());
                Logger.debug(me.getKey()+", "+me.getValue());
            }
        }
        Logger.debug("+++ resolving..."+req.getUrl());
        try {
            WSResponse res;
            if(params!=null && params.containsKey("keywords"))
            {
                Logger.debug("doing the concept");
                res = req.post("").toCompletableFuture().get();
                JsonNode json = res.asJson();
                Logger.debug(json.textValue());
                String queryId = json.get("queryId").asText();
                resolve ("https://kba.ncats.io/concepts/data/"+queryId,params,kn,kg,resolver);
            }
            else
            {
                Logger.debug ("not doing the concept "+url);
                res = req.get().toCompletableFuture().get();

            }
            JsonNode json = res.asJson();
            String queryId = json.get("queryId").asText();
            prettyPrint(json);
            resolver.resolve(json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }

    void resolveDiseases (JsonNode json, KNode kn, KGraph kg) {
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveDiseases(json.get(i), kn, kg);
        }
        else {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "disease");
            props.put(URI_P, ksp.getUri()+"/concepts/data/"+URLEncoder.encode(json.get("id").asText()));
            props.put(NAME_P, json.get("name").asText());
            List<String> syns = new ArrayList<>();
            JsonNode sn = json.get(SYNONYMS_P);
            if (sn != null) {
                for (int i = 0; i < sn.size(); ++i)
                    syns.add(sn.get(i).asText());
                props.put(SYNONYMS_P, syns.toArray(new String[0]));
            }
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                kg.createEdgeIfAbsent(kn, xn, "resolve");
            }
        }
    }
    //TODO convert this method to use POST instead of GET as is the standard with Beacons now
    void resolveQuery (JsonNode json, KNode kn, KGraph kg)
    {
        Logger.debug("resolveQuery");
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveQuery(json.get(i), kn, kg);
        }
        else {
//            String semantic = json.get("semanticGroup").asText();
//            String type = getBeaconType(semantic);
//            Logger.debug("TYPE: "+type);
//            String clique = unquote(json.get("clique").toString());
//            String beacondId = unquote(json.get("id").toString());
//            props.put(TYPE_P,type);
//            props.put(URI_P, ksp.getUri()+"/concepts/data/"+URLEncoder.encode(clique));
//            props.put(NAME_P, json.get("name").asText());
//            List<String> syns = new ArrayList<>();
//            JsonNode sn = json.get(SYNONYMS_P);
//            props.put("clique",clique);
//            props.put("beaconId",beacondId);
//            if (sn != null) {
//                for (int i = 0; i < sn.size(); ++i)
//                    syns.add(sn.get(i).asText());
//                props.put(SYNONYMS_P, syns.toArray(new String[0]));
//            }
            prettyPrint(json);
            JsonNode results = json.get("results");
            Logger.debug("Resolve Query Results:");
            for(int i = 0;i<results.size();++i)
            {
                prettyPrint(results.get(i));
                JsonNode currentResult = results.get(i);
                String clique=currentResult.get("clique").asText();
                String name = currentResult.get("name").asText();
                String category = currentResult.get("category").asText();
                Map<String, Object> props = new TreeMap<>();
                props.put(TYPE_P,category);
                props.put(URI_P,ksp.getUri()+"/concepts/data/"+URLEncoder.encode(clique));
                props.put(NAME_P,name);
                KNode xn = kg.createNodeIfAbsent(props, URI_P);
                if (xn.getId() != kn.getId()) {
                    xn.addTag("KS:"+ksp.getId());
                    kg.createEdgeIfAbsent(kn, xn, "resolve");
                }
            }
        }
    }

//    void resolveConcept(JsonNode json, KNode kn, KGraph kg)
//    {
//        Logger.debug("Resolve Concept");
//        if (json.isArray()) {
//            for (int i = 0; i < json.size(); ++i)
//                resolveConcept(json.get(i), kn, kg);
//        }
//        else
//        {
//            Map<String, Object> props = new TreeMap<>();
//            JsonNode subject = json.get("subject");
//            JsonNode predicate = json.get("predicate");
//            JsonNode object = json.get("object");
//            JsonNode newNode = null;
//
//            String knId = unquote(kn.get("beaconId").toString());
//            String objId = unquote(object.get("id").toString());
//            String subId = unquote(subject.get("id").toString());
//            String knClique = unquote(kn.get("clique").toString());
//            String objClique = unquote(object.get("clique").toString());
//            String subClique = unquote(subject.get("clique").toString());
//            String action = unquote(predicate.get("name").toString());
//            if(knId.equals(objId)
//                    && !knId.equals(subId))
//            {
//                newNode = subject;
//            }
//            else if(knId.equals(subId) &&
//                    !knId.equals(objId))
//            {
//                newNode = object;
//            }
//            else if(knClique.equals(objClique))
//            {
//                newNode = subject;
//            }
//            else if(knClique.equals(subClique))
//            {
//                newNode = object;
//            }
//            prettyPrint(object);
//            prettyPrint(predicate);
//            prettyPrint(subject);
//
//            String id = unquote(newNode.get("id").toString());
//            String clique = unquote(newNode.get("clique").toString());
//            String type = getBeaconType(unquote(newNode.get("semanticGroup").toString()));
//            String name = unquote(newNode.get("name").toString());
//            props.put("beaconId",id);
//            props.put("clique",clique);
//            props.put(TYPE_P,type);
//            props.put(NAME_P,name);
//            props.put(URI_P, ksp.getUri()+"/concepts/data/"+URLEncoder.encode(id));
//            KNode xn = kg.createNodeIfAbsent(props,URI_P);
//            if(xn.getId() != kn.getId())
//            {
//                xn.addTag("KS:"+ksp.getId());
//                kg.createEdgeIfAbsent(kn,xn,action);
//            }
//        }
//    }
//    void seedConcept(String id, KNode kn, KGraph kg)
//    {
//        Logger.debug("Seed Concept");
//        try {
//            Map<String, String> q = new HashMap<>();
//            q.put("source",id);
//            resolve(ksp.getUri()+"/statements/data/",q,kn,kg,this::resolveConcept);
//        }
//        catch (Exception ex) {
//            Logger.error("Can't resolve url: "+ksp.getUri()+"/statements?source="+id, ex);
//        }
//    }
    void resolveConcept(JsonNode json, KNode kn, KGraph kg)
    {

        Logger.debug("Resolve Concept");
        prettyPrint(json);
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i)
                resolveConcept(json.get(i), kn, kg);
        }
        else
        {
            String queryId = json.get("queryId").asText();
            Logger.debug("queryId="+queryId);
            resolve(ksp.getUri()+"/concepts/data/"+queryId,null,kn,kg,this::resolveQuery);
        }
    }
    void seedConcept(String keyword, KNode kn, KGraph kg)
    {
        Logger.debug("Seed Concept");
        try
        {
            Map<String, String> q = new HashMap<>();
            q.put("keywords",keyword);
            resolve(ksp.getUri()+"/concepts",q,kn,kg,this::resolveConcept);
        }
        catch(Exception ex)
        {
            Logger.error("Can't resolve url: "+ksp.getUri()+"/concepts?keywords="+keyword);
        }
    }
    public void prettyPrint (JsonNode json)
    {
        StringBuilder sb = new StringBuilder();
        Iterator<String> i = json.fieldNames();
        while(i.hasNext())
        {
            String next = i.next();
            sb.append(next);
            sb.append(": ");
            sb.append(json.get(next));
            sb.append("\n");
        }
        System.out.println(sb.toString());
    }
    public String unquote (String s)
    {
        return s.replace("\"","");
    }
    protected String getBeaconType(String s)
    {
        String type = SEMANTIC_GROUPS.get(s);
        if(type == null || type.isEmpty())
        {
            if(!s.isEmpty()&&s!=null)
            {
                type = s;

            }
            else {
                type = "UNKNOWN";
            }

        }
        return type;
    }
    //protected abstract void resolve (JsonNode json, KNode kn, KGraph kgraph);
}
