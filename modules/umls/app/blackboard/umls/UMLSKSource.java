package blackboard.umls;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.regex.*;
import java.util.*;
import java.net.URLEncoder;
import java.util.concurrent.Callable;

import com.fasterxml.jackson.databind.JsonNode;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import akka.actor.ActorSystem;

import blackboard.*;
import play.mvc.BodyParser;
import play.mvc.Http;

import static blackboard.KEntity.*;

public class UMLSKSource implements KSource {
    public final WSClient wsclient;
    public final KSourceProvider ksp;
    private final CacheApi cache;
    private final TGT tgt;
    
    private final String APIKEY;
    private final String APIVER;

    class Request {
        final WSRequest req;
        Request (String context) throws Exception {
            req = wsclient.url(ksp.getUri()+"/"+context);
        }
        
        WSRequest q (String param, String value) {
            return req.setQueryParameter(param, value);
        }
    }

    /*
     * ticket granting ticket
     */
    class TGT {
        String url;
        TGT () throws Exception {
            url = getTGTUrl ();
        }

        String ticket () throws Exception {
            WSResponse res = wsclient.url(url)
                .setContentType("application/x-www-form-urlencoded")
                .post("service=http%3A%2F%2Fumlsks.nlm.nih.gov")
                .toCompletableFuture().get();
            
            int status = res.getStatus();
            String body = res.getBody();
            
            if (status == 201 || status == 200)
                return body;

            Logger.debug("ticket: status="+status+" body="+body);
            try {
                // get new tgt url
                url = getTGTUrl ();
                return ticket ();
            }
            catch (Exception ex) {
                Logger.error("Can't retrieve TGT url", ex);
            }
            return null;
        }
    }
    
    String getTGTUrl () throws Exception {
        WSResponse res = wsclient.url
            ("https://utslogin.nlm.nih.gov/cas/v1/api-key")
            .setFollowRedirects(true)
            .setContentType("application/x-www-form-urlencoded")
            .post("apikey="+APIKEY)
            .toCompletableFuture().get();

        String url = null;
        int status = res.getStatus();
        String body = res.getBody();                
        if (status == 201 || status == 200) {
            int pos = body.indexOf("https");
            if (pos > 0) {
                int end = body.indexOf("-cas");
                if (end > pos) {
                    url = body.substring(pos, end+4);
                }
            }
            else {
                Logger.error("Unexpected response: "+body);
            }
        }
        
        Logger.debug("+++ retrieving TGT.. "
                     +(url != null ? url : (status+" => "+body)));

        return url;
    }            
    
    
    @Inject
    public UMLSKSource (WSClient wsclient, CacheApi cache,
                        @Named("umls") KSourceProvider ksp,
                        ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;

        Map<String, String> props = ksp.getProperties();
        APIKEY = props.get("apikey");
        if (APIKEY == null)
            throw new IllegalArgumentException
                ("No UMLS \"apikey\" specified!");

        APIVER = props.containsKey("umls-version")
            ? props.get("umls-version") : "current";

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });

        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
        try {
            tgt = new TGT ();
        }
        catch (Exception ex) {
            throw new RuntimeException (ex);
        }
    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        try {
            for (KNode kn : nodes) {
                switch (kn.getType()) {
                case "query":
                    seedQuery ((String) kn.get("term"), kn, kgraph);
                    break;

                case "concept":
                    expand (kgraph, kn);
                    break;
                    
                default:
                    { String name = (String)kn.get("name");
                        if (name != null) {
                            seedQuery (name, kn, kgraph);
                        }
                        else {
                            Logger.debug(ksp.getId()
                                         +": can't resolve node of type \""
                                         +kn.getType()+"\"");
                        }
                    }
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't execute knowledge source on kgraph "
                         +kgraph.getId(), ex);            
        }
    }

    protected void seedQuery (String query, KNode kn, KGraph kg)
        throws Exception {
        WSRequest req = search (query);
        WSResponse res = req.get().toCompletableFuture().get();
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri()+": status="+res.getStatus());
        }
        else {
            JsonNode results = res.asJson().get("result").get("results");
            for (int i = 0; i < results.size(); ++i) {
                JsonNode n = results.get(i);
                KNode xn = createConceptNodeIfAbsent (kg, n.get("ui").asText());
                Map<String, Object> props = new HashMap<>();
                props.put("value", req.getUrl()+"?string="+query);
                kg.createEdgeIfAbsent(kn, xn, "resolve", props, null);
            }
        }
    }

    public KNode createConceptNodeIfAbsent (KGraph kg, String cui)
        throws Exception {
        JsonNode n = getCui (cui);
        if (n == null)
            return null;
            
        Map<String, Object> props = new HashMap<>();
        props.put("cui", n.get("ui").asText());
        String uri = n.get("atoms").asText();
        int pos = uri.lastIndexOf('/');
        if (pos > 0) {
            uri = uri.substring(0, pos);
        }
        
        props.put(URI_P, uri);
        props.put(NAME_P, n.get("name").asText());
        props.put(TYPE_P, "concept");
        List<String> types = new ArrayList<>();
        Set<String> semtypes = new TreeSet<>();
        JsonNode sn = n.get("semanticTypes");        
        for (int i = 0; i < sn.size(); ++i) {
            String t = sn.get(i).get("uri").asText();
            pos = t.lastIndexOf('/');
            if (pos > 0)
                t = t.substring(pos+1);
            types.add(t);
            semtypes.add(sn.get(i).get("name").asText());
        }
        props.put("semtypes", types.toArray(new String[0]));
        KNode kn = kg.createNodeIfAbsent(props, URI_P);
        for (String t : kn.getTags())
            semtypes.remove(t);

        if (!semtypes.isEmpty())
            kn.addTag(semtypes.toArray(new String[0]));
        
        return kn;
    }

    public Map<String, String> getRelatedCuis (String cui) throws Exception {
        JsonNode json = getContent (cui, "relations");
        Map<String, String> relations = new HashMap<>();
        if (json != null) {
            for (int i = 0; i < json.size(); ++i) {
                JsonNode n = json.get(i);
                if (!n.get("obsolete").asBoolean()) {
                    String rel = n.get("relationLabel").asText();
                    String uri = n.get("relatedId").asText();
                    String ui = n.get("ui").asText();
                    int pos = uri.lastIndexOf('/');
                    if (pos > 0)
                        uri = uri.substring(pos+1);
                    relations.put(ui, rel+":"+uri);
                }
            }
        }
        return relations;
    }

    public List<KEdge> expand (KGraph kg, KNode node, String... types)
        throws Exception {
        String cui = (String) node.get("cui");
        if (cui == null)
            throw new IllegalArgumentException
                ("Node doesn't have CUI defined!");
        Set<String> _types = new HashSet<>();
        for (String t : types)
            _types.add(t.toUpperCase());
        
        List<KEdge> edges = new ArrayList<>();
        Map<String, String> relations = getRelatedCuis (cui);
        for (Map.Entry<String, String> me : relations.entrySet()) {
            String[] toks = me.getValue().split(":");
            if (_types.isEmpty() || _types.contains(toks[0])) {
                KNode n = createConceptNodeIfAbsent (kg, toks[1]);
                KEdge edge = kg.createEdgeIfAbsent(node, n, me.getKey());
                edge.put("scope", toks[0]);
                edges.add(edge);
            }
        }
        return edges;
    }

    public String ticket () throws Exception {
        return tgt.ticket();
    }

    public WSRequest search (String query) throws Exception {
        String ticket = tgt.ticket();
        String url = ksp.getUri()+"/search/"+APIVER;
            
        Logger.debug("++ ticket="+ticket+" query="+query);
        return wsclient.url(url)
            .setQueryParameter("string", query.replaceAll("%20", "+"))
            .setQueryParameter("ticket", ticket)
            ;
    }

    public WSRequest cui (String cui) throws Exception {
        String ticket = tgt.ticket();
        String url = ksp.getUri()+"/content/"+APIVER+"/CUI/"+cui;
        Logger.debug("++ CUI: ticket="+ticket+" cui="+cui);
        return wsclient.url(url).setQueryParameter("ticket", ticket);
    }

    public WSRequest content (String cui, String context) throws Exception {
        String ticket = tgt.ticket();
        String url = ksp.getUri()+"/content/"+APIVER+"/CUI/"+cui+"/"+context;
        Logger.debug("++ "+context+": ticket="+ticket+" cui="+cui);
        return wsclient.url(url).setQueryParameter("ticket", ticket);
    }

    public WSRequest source (String src, String id, String context)
        throws Exception {
        String ticket = tgt.ticket();
        String url = ksp.getUri()+"/content/"+APIVER+"/source/"+src+"/"+id;
        if (context != null)
            url += "/"+context;
        Logger.debug("++ "+src+": ticket="+ticket
                     +" id="+id+" context="+context);
        return wsclient.url(url).setQueryParameter("ticket", ticket);
    }

    public JsonNode getCui (final String cui) throws Exception {
        return cache.getOrElse("umls/"+cui, new Callable<JsonNode> () {
                public JsonNode call () throws Exception {
                    WSResponse res =
                        cui(cui).get().toCompletableFuture().get();
                    try {
                        if (res.getStatus() == 200) {
                            return res.asJson().get("result");
                        }
                    }
                    catch (Exception ex) {
                        Logger.error("Can't retrieve CUI "+cui+" ==> "
                                     +res.getBody(), ex);
                    }
                    Logger.warn("** Can't retrieve CUI "+cui);
                    return null;
                }
            });
    }

    public JsonNode getSource (final String src, final String id,
                               final String context)
        throws Exception {
        return cache.getOrElse
            ("umls/"+src+"/"+id+(context!=null?context:""),
             new Callable<JsonNode> () {
                 public JsonNode call () throws Exception {
                     WSResponse res = source(src, id, context)
                         .get().toCompletableFuture().get();
                     return res.getStatus() == 200
                         ? res.asJson().get("result") : null;
                 }
             });
    }

    public JsonNode getContent (final String cui, final String context)
        throws Exception {
        return cache.getOrElse
            ("umls/"+context+"/"+cui, new Callable<JsonNode>() {
                    public JsonNode call () throws Exception {
                        WSResponse res = content(cui, context)
                            .get().toCompletableFuture().get();
                        return 200 == res.getStatus()
                            ? res.asJson().get("result") : null;
                    }
                });
    }

    public JsonNode getSearch (final String query,
                               final int skip, final int top) throws Exception {
        return cache.getOrElse
            ("umls/search/"+query+"/"+top+"/"+skip, new Callable<JsonNode>() {
                    public JsonNode call () throws Exception {
                        WSResponse res = search(query)
                            .setQueryParameter("pageSize", String.valueOf(top))
                            .setQueryParameter("pageNumber",
                                               String.valueOf(skip/top+1))
                            .get().toCompletableFuture().get();
                        if (200 == res.getStatus()) {
                            return res.asJson().get("result").get("results");
                        }
                        return null;
                    }
                });
    }
}
