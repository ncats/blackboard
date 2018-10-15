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

import blackboard.pubmed.index.PubMedIndex;
import blackboard.pubmed.index.PubMedIndexFactory;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;


@Singleton
public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final PubMedIndex pmi;
    final MeshDb mesh;

    @Inject
    public Controller (HttpExecutionContext ec, PubMedIndexFactory pmif,
                       Configuration conf, MeshKSource meshKS,
                       ApplicationLifecycle lifecycle) {
        String dbdir = conf.getString("app.pubmed.index");
        this.pmi = dbdir != null ? pmif.get(new File (dbdir)) : null;
        this.ec = ec;
        
        mesh = meshKS.getMeshDb();
        lifecycle.addStopHook(() -> {
                if (pmi != null)
                    pmi.close();
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug(getClass().getName()+": "+pmi);
    }
    
    public Result index () {
        return ok (blackboard.pubmed.views.html.mock.index.render());
    }

    public CompletionStage<Result> search (String q) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                try {
                    PubMedIndex.SearchResult result = pmi.search(q);
                    return ok (Json.toJson(result.docs));
                }
                catch (Exception ex) {
                    return internalServerError
                        (views.html.ui.status.render(ex.getMessage(),
                                                     "Server Error"));
                }
            }, ec.current());
    }
}
