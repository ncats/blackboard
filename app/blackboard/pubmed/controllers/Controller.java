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
import blackboard.pubmed.index.PubMedIndexFactory;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;


public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final PubMedIndexManager pmim;

    @Inject
    public Controller (HttpExecutionContext ec, PubMedIndexManager pmim,
                       ApplicationLifecycle lifecycle) {
        this.pmim = pmim;
        this.ec = ec;
        
        lifecycle.addStopHook(() -> {
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$$" +getClass().getName()+": "+pmim);
    }
    
    public Result index () {
        return ok (blackboard.pubmed.views.html.mock.index.render());
    }

    /*
    public CompletionStage<Result> search (String q) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                try {
                    PubMedIndex.SearchResult result = pmi.search(q, null);
                    return ok (Json.toJson(result.docs));
                }
                catch (Exception ex) {
                    return internalServerError
                        (views.html.ui.status.render(ex.getMessage(),
                                                     "Server Error"));
                }
            }, ec.current());
    }
    */
}
