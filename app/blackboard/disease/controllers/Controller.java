package blackboard.disease.controllers;

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
import play.Logger;
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

import blackboard.disease.*;

public class Controller extends play.mvc.Controller {
    final protected HttpExecutionContext ec;
    final protected ObjectMapper mapper = Json.mapper();
    final protected SyncCacheApi cache;
    final protected DiseaseKSource disease;

    @Inject
    public Controller (HttpExecutionContext ec, DiseaseKSource disease,
                       Configuration config, SyncCacheApi cache,
                       ApplicationLifecycle lifecycle) {
        this.ec = ec;
        this.cache = cache;
        this.disease = disease;

        lifecycle.addStopHook(() -> {
                return CompletableFuture.completedFuture(null);
            });
        
        Logger.debug("$$" +getClass().getName());
    }

    public CompletionStage<Result> search (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        if (q == null || "".equals(q)) {
            return supplyAsync (() -> {
                    return redirect
                        (blackboard.controllers.routes.KnowledgeApp.index());
                }, ec.current());
        }

        return supplyAsync (() -> {
                try {
                    return _search (q, skip, top);
                }
                catch (Exception ex) {
                    Logger.error("** "+request().uri()+" failed", ex);
                    return ok (views.html.ui.error.render
                               (ex.getMessage(), 500));
                }
            }, ec.current());
    }

    public Result _search (String q, int skip, int top) {
        DiseaseResult result = disease.search(q, skip, top);
        return ok ((JsonNode)mapper.valueToTree(result));
    }
}
