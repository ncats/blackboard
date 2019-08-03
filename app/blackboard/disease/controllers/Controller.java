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
import blackboard.utils.Util;
import blackboard.disease.views.html.*;

public class Controller extends play.mvc.Controller {
    static final JsonNode EMPTY_JSON = Json.newObject();
    final protected HttpExecutionContext ec;
    final protected ObjectMapper mapper = Json.mapper();
    final protected SyncCacheApi cache;
    final protected DiseaseKSource dks;

    @Inject
    public Controller (HttpExecutionContext ec, DiseaseKSource dks,
                       Configuration config, SyncCacheApi cache) {
        this.ec = ec;
        this.cache = cache;
        this.dks = dks;
        
        Logger.debug("$$" +getClass().getName());
    }

    public CompletionStage<Result> disease (Long id) {
        Logger.debug(">> "+request().uri());
        return dks.getDisease(id)
            .thenApplyAsync(d -> ok (diseasepage.render(Controller.this, d)),
                            ec.current());
    }

    public CompletionStage<Result> resolve (String name) {
        CompletionStage<Disease> result = dks.resolve(name);
        return result.thenApplyAsync(r -> {
                if (!r.isEmpty()) {
                    JsonNode json = mapper.valueToTree(r);
                    String path = request().getQueryString("path");
                    if (path != null)
                        json = json.at(path);
                    return ok (json.isMissingNode() ? EMPTY_JSON : json);
                }
                return ok (EMPTY_JSON);
            }, ec.current());
    }
    
    public CompletionStage<Result> search (String q, int skip, int top) {
        CompletionStage<DiseaseResult> result = dks.search(q, skip, top);
        return result.thenApplyAsync(r -> {
                JsonNode json = mapper.valueToTree(r);
                String path = request().getQueryString("path");
                if (path != null)
                    json = json.at(path);
                return ok (json.isMissingNode() ? EMPTY_JSON : json);
            }, ec.current());
    }

    public CompletionStage<Result> _diseases (String q, int skip, int top) {
        CompletionStage<DiseaseResult> result = dks.search(q, skip, top);
        return result.thenApplyAsync(r -> {
                int page = skip / top + 1;
                int[] pages = Util.paging(top, page, r.total);
                Map<Integer, String> urls = new TreeMap<>();
                for (int i = 0; i < pages.length; ++i) {
                    Call call = routes.Controller.diseases
                        (q, (pages[i]-1)*top, top);
                    urls.put(pages[i], Util.getURL(call, request ()));
                }
                
                return ok (blackboard.disease.views.html.diseases.render
                           (page, pages, urls, r));
            }, ec.current());
    }
    
    public CompletionStage<Result> diseases (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        if (q == null || "".equals(q)) {
            return supplyAsync (() -> {
                    return redirect
                        (blackboard.controllers.routes.KnowledgeApp.index());
                }, ec.current());
        }
        return  _diseases (q, skip, top);
    }

    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("diseaseRoutes",
                    routes.javascript.Controller.search(),
                    routes.javascript.Controller.disease(),
                    routes.javascript.Controller.resolve()
                    )).as("text/javascript");
    }
}
