package controllers.umls;

import java.util.*;
import java.util.regex.*;
import java.util.concurrent.CompletionStage;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.core.JsonParseException;

import blackboard.umls.UMLSKSource;
import blackboard.umls.Concept;
import blackboard.umls.MatchedConcept;
import blackboard.umls.DataSource;

@Singleton
public class Controller extends play.mvc.Controller {
    final UMLSKSource ks;
    final HttpExecutionContext ec;
    
    @Inject
    public Controller (HttpExecutionContext ec, UMLSKSource ks) {
        this.ks = ks;
        this.ec = ec;
    }

    static protected CompletionStage<Result> async (Result result) {
        return supplyAsync (() -> {
                return result;
            });
    }
    
    public Result index () {
        return ok (views.html.umls.index.render(ks));
    }

    public CompletionStage<Result> cui (String cui) {
        return supplyAsync (() -> {
                try {
                    Concept concept = ks.getConcept(cui);
                    if (concept != null)
                        return ok (views.html.umls.cui.render(concept, ks));
                    return ok (views.html.core.notfound.render
                               ("Unknown concept <code>"+cui+"</code>"));
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve concept for "+cui, ex);
                    return ok (views.html.core.error.render
                               ("Can't retrieve concept for "+cui+": "
                                +ex.getMessage(), 500));
                }
            }, ec.current());
    }

    public CompletionStage<Result> keyTicket () {
        return supplyAsync (() -> {
                try {
                    return ok (ks.ticket());
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    static ObjectNode toObject (JsonNode node) {
        Map<String, JsonNode> fields = new HashMap<>();
        for (Iterator<Map.Entry<String, JsonNode>> it =
                 node.fields(); it.hasNext();) {
            Map.Entry<String, JsonNode> me = it.next();
            fields.put(me.getKey(), me.getValue());
        }
        ObjectNode n = Json.newObject();
        n.setAll(fields);
        return n;
    }
    
    static ObjectNode instrument (JsonNode node) {
        ObjectNode n = toObject (node);
        String cui = n.get("ui").asText();
        n.put("_cui", controllers.umls.routes.Controller.keyCui(cui).url());
        n.put("_atoms", routes.Controller.keyAtoms(cui).url());
        n.put("_definitions", routes.Controller.keyDefinitions(cui).url());
        n.put("_relations", routes.Controller.keyRelations(cui).url());
        
        return n;
    }

    public CompletionStage<Result> keySearch
        (final String q, final Integer skip, final Integer top) {
        return supplyAsync (() -> {
                try {
                    WSResponse res = ks.search(q)
                        .setQueryParameter("pageSize", top.toString())
                        .setQueryParameter("pageNumber",
                                           String.valueOf(skip/top+1))
                        .get().toCompletableFuture().get();
                    JsonNode json = res.asJson();
                    if (json.hasNonNull("result")) {
                        JsonNode results = json.get("result").get("results");
                        ArrayNode ary = Json.newArray();
                        for (int i = 0; i < results.size(); ++i) {
                            JsonNode r = results.get(i);
                            ary.add(instrument (r));
                        }
                        json = ary;
                    }
                    else
                        json = Json.newArray();
                    return status (res.getStatus(), json);
                }
                catch (Exception ex) {
                    Logger.error("Can't process search \""+q+"\"", ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current()).exceptionally(ex -> {
                    return internalServerError (ex.getMessage());
                });
    }

    public CompletionStage<Result> keyCui (String cui) {
        return supplyAsync (() -> {
                try {
                    JsonNode json = ks.getCui(cui);
                    return json != null
                        ? ok (Json.prettyPrint(json)).as("application/json")
                        : notFound("Request not matched: "+request().uri());
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve "+cui, ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> keyFull (String cui) {
        return supplyAsync (() -> {
                try {
                    JsonNode json = ks.getCui(cui);
                    if (json != null) {
                        ObjectNode n = toObject (json);
                        n.put("definitions", ks.getContent(cui, "definitions"));
                        n.put("atoms", ks.getContent(cui, "atoms"));
                        n.put("relations", ks.getContent(cui, "relations"));
                        json = n;
                    }
                    
                    return json != null ? ok (json)
                        : notFound("Request not matched: "+request().uri());
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve "+cui, ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    CompletionStage<Result> content (final String cui, final String context) {
        return supplyAsync (() -> {
                try {
                    JsonNode json = ks.getContent(cui, context);
                    return json != null ? ok (json)
                        : notFound("Request not matched: "+request().uri());
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> keyAtoms (String cui) {
        return content (cui, "atoms");
    }
    
    public CompletionStage<Result> keyDefinitions (String cui) {
        return content (cui, "definitions");
    }
    
    public CompletionStage<Result> keyRelations (String cui) {
        return content (cui, "relations");
    }

    public CompletionStage<Result> keySource (final String src,
                                              final String id) {
        return supplyAsync (() -> {
                try {
                    WSResponse res = ks.source(src, id, "atoms/preferred")
                        .get().toCompletableFuture().get();
                    try {
                        JsonNode json = res.asJson();
                        if (json.hasNonNull("result")) {
                            return status (res.getStatus(), json.get("result"));
                        }
                        return notFound
                            ("Request not matched: "+request().uri());
                    }
                    catch (RuntimeException ex) {
                        Logger.error
                            ("Can't parse json for "+id+" for source "+src, ex);
                        return internalServerError (res.getBody());
                    }
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve "+id+" for source "+src, ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> apiFindConcepts
        (final String term, final Integer skip, final Integer top) {
        return supplyAsync (() -> {
                try {
                    List<MatchedConcept> results =
                        ks.findConcepts(term, skip, top);
                    return ok (Json.prettyPrint(Json.toJson(results)))
                        .as("application/json");
                }
                catch (Exception ex) {
                    Logger.error("findConcepts: "+ex.getMessage(), ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    /*
     * src can be one of
     * cui - concept ui
     * lui - term id
     * sui - string id
     * aui - atom id
     * scui - source concept id (e.g., mesh concept ui)
     * sdui - source descriptor id (e.g., mesh descriptor ui)
     */
    public CompletionStage<Result> apiConcept (final String src,
                                               final String id) {
        return supplyAsync (() -> {
                try {
                    Concept concept = ks.getConcept(src.toLowerCase(), id);
                    return concept != null
                        ? ok (Json.prettyPrint(Json.toJson(concept)))
                        .as("application/json")
                        : notFound ("Can't locate concept for "+src+"/"+id);
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> apiDatasources () {
        return supplyAsync (() -> {
                try {
                    Map<String, List<Map<String, String>>> data =
                        new TreeMap<>();
                    for (DataSource ds : ks.getDataSources()) {
                        List<Map<String, String>> s = data.get(ds.name);
                        if (s == null) {
                            s = new ArrayList<>();
                            data.put(ds.name, s);
                        }
                        Map<String, String> m = new TreeMap<>();
                        m.put("version", ds.version);
                        m.put("description", ds.description);
                        s.add(m);
                    }
                    
                    return ok (Json.prettyPrint(Json.toJson(data)))
                        .as("application/json");
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> apiDatasource (String name) {
        return supplyAsync (() -> {
                try {
                    List<Map<String, String>> data = new ArrayList<>();
                    for (DataSource ds : ks.getDataSources()) {
                        if (name.equalsIgnoreCase(ds.name)) {
                            Map<String, String> m = new TreeMap<>();
                            m.put("version", ds.version);
                            m.put("description", ds.description);
                            data.add(m);
                        }
                    }
                    
                    return ok (Json.toJson(data));
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("umlsRoutes",
                    routes.javascript.Controller.apiConcept(),
                    routes.javascript.Controller.apiFindConcepts(),
                    routes.javascript.Controller.cui()
                    )).as("text/javascript");
    }
}

