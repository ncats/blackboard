package blackboard.pubmed.controllers;

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

import blackboard.pubmed.index.PubMedIndexManager;
import blackboard.pubmed.index.PubMedIndex;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;


public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final PubMedIndexManager indexManager;

    @Inject
    public Controller (HttpExecutionContext ec, PubMedIndexManager indexManager,
                       ApplicationLifecycle lifecycle) {
        this.indexManager = indexManager;
        this.ec = ec;
        
        lifecycle.addStopHook(() -> {
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$$" +getClass().getName()+": "+indexManager);
    }
    
    public Result index () {
        return ok (blackboard.pubmed.views.html.mock.index.render());
    }

    public CompletionStage<Result> search (String q) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                try {
                    PubMedIndex.SearchResult[] results =
                        indexManager.search(q, null);
                    ArrayNode json = Json.newArray();
                    for (PubMedIndex.SearchResult r : results)
                        json.add(Json.toJson(r.docs));
                    
                    return ok (json);
                }
                catch (Exception ex) {
                    return internalServerError
                        (views.html.ui.status.render(ex.getMessage(),
                                                     "Server Error"));
                }
            }, ec.current());
    }

    public CompletionStage<Result> pmid (Long pmid, String format) {
        return supplyAsync (() -> {
                try {
                    byte[] doc = indexManager.getDoc(pmid, format);
                    return ok(doc).as("application/xml");
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve doc: "+pmid, ex);
                    return internalServerError
                        ("Internal server error: "+ex.getMessage());
                }
            }, ec.current());
    }
}
