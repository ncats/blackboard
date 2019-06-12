package blackboard.pubmed.controllers;

import java.lang.reflect.Array;
import java.util.*;
import java.io.File;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.Environment;
import play.mvc.BodyParser;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;
import play.inject.ApplicationLifecycle;
import play.cache.SyncCacheApi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.lang3.StringUtils;

import blackboard.index.pubmed.PubMedIndexManager;
import blackboard.index.pubmed.PubMedIndex;
import static blackboard.index.pubmed.PubMedIndex.*;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;

public class Controller extends play.mvc.Controller {
    final protected HttpExecutionContext ec;
    final protected PubMedIndexManager pubmed;
    final protected ObjectMapper mapper = Json.mapper();
    final protected SyncCacheApi cache;

    @Inject
    public Controller (HttpExecutionContext ec, PubMedIndexManager pubmed,
                       SyncCacheApi cache, ApplicationLifecycle lifecycle) {
        this.pubmed = pubmed;
        this.ec = ec;
        this.cache = cache;

        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        lifecycle.addStopHook(() -> {
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$$" +getClass().getName()+": "+pubmed);
    }
    
    public Result index () {
        return ok (blackboard.pubmed.views.html.mock.index.render());
    }

    Map<String, Object> parseFacets () {
        Map<String, Object> facets = null;
        String[] params = request().queryString().get("facet");
        if (params != null) {
            facets = new TreeMap<>();
            for (String p : params) {
                int pos = p.indexOf('/');
                if (pos > 0) {
                    String name = p.substring(0, pos);
                    Object value = p.substring(pos+1);
                    switch (name) {
                    case FACET_TR:
                    case FACET_GRANTAGENCY:
                        value = ((String)value).split("\\.");
                        break;
                    default:
                    }
                    
                    Object old = facets.get(name);
                    if (old != null) {
                        if (old instanceof String[]) {
                            int len = Array.getLength(old);
                            if (value instanceof String[]) {
                                Object[] values = new Object[]{
                                    old, value
                                };
                                facets.put(name, values);
                            }
                            else {
                                String[] values = new String[len+1];
                                for (int i = 0; i < len; ++i)
                                    values[i] = (String)Array.get(old, i);
                                values[len] = (String) value;
                                facets.put(name, values);
                            }
                        }
                        else if (old instanceof Object[]) {
                            int len = Array.getLength(old);
                            Object[] values = new Object[len+1];
                            for (int i = 0; i < len; ++i)
                                values[i] = Array.get(old, i);
                            values[len] = value;
                            facets.put(name, values);
                        }
                        else {
                            if (value instanceof String[]) {
                                facets.put(name, new Object[]{
                                        old, value
                                    });
                            }
                            else {
                                facets.put(name, new String[]{
                                        (String)old,
                                        (String)value
                                    });
                            }
                        }
                    }
                    else {
                        facets.put(name, value);
                    }
                }
                else {
                    Logger.warn("Not a valid facet: "+p);
                }
            }
        }
        return facets;
    }

    JsonNode toJson (Map<String, Object> fmap) {
        /*
        ObjectNode json = Json.newObject();
        for (Map.Entry<String, Object> me : fmap.entrySet()) {
            Object value = me.getValue();
            if (value instanceof Object[]) {
                Object[] values = (Object[])value;
                List<String> vs = new ArrayList<>();
                for (Object v : values) {
                    if (v instanceof String[]) {
                        vs.add(StringUtils.join((String[])v, '.'));
                    }
                    else
                        vs.add((String)v);
                }
                json.put(me.getKey(), mapper.valueToTree(vs));
            }
            else {
                json.put(me.getKey(), mapper.valueToTree(value));
            }
        }
        return json;
        */
        return mapper.valueToTree(fmap);
    }

    public CompletionStage<Result> filter (int skip, int top) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                try {
                    Map<String, Object> facets = parseFacets ();
                    if (facets == null || facets.isEmpty())
                        return badRequest ("No facets specified!");
                    
                    SearchResult result =
                        pubmed.search(facets, skip, top);
                    ObjectNode json = Json.newObject();
                    json.put("query", mapper.valueToTree(result.query));
                    json.put("count", result.size());
                    json.put("total", result.total);
                    ObjectNode content = (ObjectNode)mapper.valueToTree(result);
                    content.remove("query");
                    content.remove("count");
                    content.remove("total");
                    json.put("content", content);
                    return ok (json);
                }
                catch (Exception ex) {
                    Logger.error("Search failed", ex);
                    return internalServerError
                        ("Internal server error: "+ex.getMessage());
                }
            }, ec.current());
    }

    public SearchResult doSearch (String q, int skip, int top)
        throws Exception {
        Map<String, Object> facets = parseFacets ();
        int slop = 1;
        try {
            String p = request().getQueryString("slop");
            if (p != null)
                slop = Integer.parseInt(p);
        }
        catch (NumberFormatException ex) {
            Logger.error("Bogus slop parameter", ex);
        }
                    
        return pubmed.search(request().getQueryString("field"), q,
                             facets, skip, top, slop);
    }
    
    public Result _search (String q, int skip, int top) throws Exception {
        ObjectNode json = Json.newObject();
        SearchResult result = doSearch (q, skip, top);
        ObjectNode query =
            (ObjectNode) mapper.valueToTree(result.query);
        query.put("rewrite", result.query.rewrite().toString());
        
        json.put("query", query);
        json.put("count", result.size());
        json.put("total", result.total);
        
        ObjectNode content = (ObjectNode)mapper.valueToTree(result);
        content.remove("query");
        content.remove("count");
        content.remove("total");
        json.put("content", content);
        
        return ok (json);
    }
    
    public CompletionStage<Result> search (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                try {
                    return _search (q, skip, top);
                }
                catch (Exception ex) {
                    Logger.error("Search failed", ex);
                    return internalServerError
                        ("Internal server error: "+ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> facets () {
        Logger.debug(">> "+request().uri());        
        return supplyAsync (() -> {
                try {
                    SearchResult result = pubmed.facets();
                    ObjectNode json = Json.newObject();
                    json.put("count", 0);
                    json.put("total", result.total);
                    ObjectNode content = (ObjectNode)mapper.valueToTree(result);
                    content.remove("query");
                    content.remove("count");
                    content.remove("total");
                    json.put("content", content);
                    return ok (json);
                }
                catch (Exception ex) {
                    Logger.error("Search failed", ex);
                    return internalServerError
                        ("Internal server error: "+ex.getMessage());
                }
            }, ec.current());
    }
    
    public CompletionStage<Result> pmid (Long pmid, String format) {
        Logger.debug(">> "+request().uri());        
        return supplyAsync (() -> {
                try {
                    MatchedDoc doc = pubmed.getDoc(pmid);
                    if (doc != null)
                        return ok(doc.toXmlString()).as("application/xml");
                    return notFound ("Can't find PMID "+pmid);
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve doc: "+pmid, ex);
                    return internalServerError
                        ("Internal server error: "+ex.getMessage());
                }
            }, ec.current());
    }

    @BodyParser.Of(value = BodyParser.Text.class)
    public CompletionStage<Result> batch (String format) {
        return supplyAsync (() -> {
                List<Long> pmids = new ArrayList<>();
                for (String t : request().body().asText().split("[,;\\s]+")) {
                    try {
                        pmids.add(Long.parseLong(t));
                    }
                    catch (NumberFormatException ex) {
                        Logger.warn("Bogus PMID: "+t);
                    }
                }
                try {
                    SearchResult result =
                        pubmed.getDocs(pmids.toArray(new Long[0]));
                    return ok(result.exportXML()).as("application/xml");
                }
                catch (Exception ex) {
                    return internalServerError
                        ("Internal server error: "+ex.getMessage());
                }
            }, ec.current());
    }
}
