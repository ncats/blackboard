package blackboard.pubmed.controllers;

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
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;
import play.inject.ApplicationLifecycle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.SerializationFeature;

import blackboard.pubmed.index.PubMedIndexManager;
import blackboard.pubmed.index.PubMedIndex;
import static blackboard.pubmed.index.PubMedIndex.*;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;


public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final PubMedIndexManager indexManager;
    final ObjectMapper mapper = Json.mapper();

    @Inject
    public Controller (HttpExecutionContext ec, PubMedIndexManager indexManager,
                       ApplicationLifecycle lifecycle) {
        this.indexManager = indexManager;
        this.ec = ec;

        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        lifecycle.addStopHook(() -> {
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$$" +getClass().getName()+": "+indexManager);
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
                        value = ((String)value).split("\\.");
                        break;
                        
                    case FACET_GRANTAGENCY:
                        value = ((String)value).split("_");
                        break;
                        
                    default:
                    }
                    facets.put(name, value);
                }
                else {
                    Logger.warn("Not a valid facet: "+p);
                }
            }
        }
        return facets;
    }

    public CompletionStage<Result> filter (int skip, int top) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                try {
                    Map<String, Object> facets = parseFacets ();
                    if (facets == null || facets.isEmpty())
                        return badRequest ("No facets specified!");
                    
                    SearchResult result =
                        indexManager.search(facets, skip, top);
                    ObjectNode json = Json.newObject();
                    json.put("filters", mapper.valueToTree(facets));
                    json.put("skip", skip);
                    json.put("top", top);
                    json.put("count", result.size());
                    json.put("total", result.total);
                    ObjectNode content = (ObjectNode)mapper.valueToTree(result);
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
    
    public CompletionStage<Result> search (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                try {
                    Map<String, Object> facets = parseFacets ();
                    SearchResult result = indexManager.search
                        (request().getQueryString("field"), q,
                         facets, skip, top);
                    ObjectNode json = Json.newObject();
                    json.put("query", q);
                    if (facets != null && !facets.isEmpty()) {
                        json.put("filters", mapper.valueToTree(facets));
                    }
                    json.put("skip", skip);
                    json.put("top", top);
                    json.put("count", result.size());
                    json.put("total", result.total);
                    ObjectNode content = (ObjectNode)mapper.valueToTree(result);
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
        return supplyAsync (() -> {
                try {
                    MatchedDoc doc = indexManager.getDoc(pmid, format);
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
}
