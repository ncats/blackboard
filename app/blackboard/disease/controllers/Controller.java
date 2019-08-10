package blackboard.disease.controllers;

import java.lang.reflect.Array;
import java.util.*;
import java.io.File;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.atomic.AtomicInteger;
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
import blackboard.umls.UMLSKSource;
import blackboard.umls.SemanticType;

public class Controller extends play.mvc.Controller {
    static final JsonNode EMPTY_JSON = Json.newObject();
    final protected HttpExecutionContext ec;
    final protected ObjectMapper mapper = Json.mapper();
    final protected SyncCacheApi cache;
    final protected DiseaseKSource dks;
    final protected UMLSKSource umls;

    @Inject
    public Controller (HttpExecutionContext ec, DiseaseKSource dks,
                       UMLSKSource umls, SyncCacheApi cache) {
        this.ec = ec;
        this.cache = cache;
        this.dks = dks;
        this.umls = umls;
        
        Logger.debug("$$" +getClass().getName());
    }

    public CompletionStage<Result> disease (String id, Long node) {
        Logger.debug(">> "+request().uri());
        if (node != null && node > 0l) {
            return dks.disease(node).thenApplyAsync
                (d -> {
                    if (!d.isEmpty() && id.equals(d.getId())) {
                        return ok (diseasepage.render(Controller.this, d));
                    }
                    throw new RuntimeException
                        ("Node "+node+" either is invalid or doesn't have id "
                         +id);
                }, ec.current()).exceptionally(ex -> {
                        Logger.warn(ex.getMessage());
                        try {
                            Disease d = dks.resolve(id)
                                .toCompletableFuture().get();
                            return !d.isEmpty()
                                ? ok (diseasepage.render(Controller.this, d))
                                : ok (blackboard.views.html.notfound
                                      .render("Unable to resolve "+id));
                        }
                        catch (Exception exx) {
                            return ok(blackboard.views.html.status
                                      .render(exx.getMessage(),
                                              "Internal server error"));
                        }
                    });
        }
        return dks.resolve(id).thenApplyAsync
            (d -> !d.isEmpty()
             ? ok (diseasepage.render(Controller.this, d))
             : ok (blackboard.views.html.notfound
                   .render("Unable to resolve "+id)),
             ec.current());
    }

    public CompletionStage<Result> resolve (String id) {
        CompletionStage<Disease> result = dks.resolve(id);
        return result.thenApplyAsync(d -> {
                if (!d.isEmpty()) {
                    JsonNode json = mapper.valueToTree(d);
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

    public List<SemanticType> getSemanticTypes (Disease d) {
        List<SemanticType> semtypes = new ArrayList<>();
        for (String t : d.getSemanticTypes()) {
            SemanticType st = umls.getSemanticType(t);
            if (st != null)
                semtypes.add(st);
        }
        return semtypes;
    }

    static void map (AtomicInteger id,
                     ObjectNode node, Map<String, String> fields) {
        for (Map.Entry<String, String> me : fields.entrySet()) {
            JsonNode value = node.remove(me.getKey());
            if (value != null) {
                node.put(me.getValue(), value);
            }
        }
        node.put("id", id.incrementAndGet());
        
        ObjectNode state = Json.newObject();
        state.put("opened", true);
        node.put("state", state);
        
        JsonNode children = node.get("children");
        if (children != null) {
            for (int i = 0; i < children.size(); ++i)
                map (id, (ObjectNode)children.get(i), fields);
        }
    }
    
    public CompletionStage<Result> tree (String id) {
        return dks.tree(id).thenApplyAsync(json -> {
                Map<String, String> fields = new TreeMap<>();
                if (id.startsWith("GARD")) {
                    fields.put("name", "text");
                }
                else if (id.startsWith("MONDO") || id.startsWith("Orphanet")) {
                    fields.put("label", "text");
                }
                else {
                    fields.put("label", "text");
                }

                AtomicInteger count = new AtomicInteger (0);
                map (count, (ObjectNode)json, fields);
                
                return ok (json);
            }, ec.current());
    }

    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("diseaseRoutes",
                    routes.javascript.Controller.search(),
                    routes.javascript.Controller.disease(),
                    routes.javascript.Controller.resolve(),
                    routes.javascript.Controller.tree()
                    )).as("text/javascript");
    }
}
