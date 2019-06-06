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
import blackboard.umls.MetaMap;
import blackboard.umls.SemanticType;


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
    
    public Result apiSemanticTypes () {
        return ok (Json.prettyPrint(Json.toJson(ks.semanticTypes)))
            .as("application/json");
    }

    public Result apiSemanticTypeLookup (String str) {
        SemanticType st = ks.getSemanticType(str);
        return st != null ? ok (Json.toJson(st))
            : notFound ("Can't lookup semantic type either by id or abbr: "
                        +str);
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

    public CompletionStage<Result> apiFindConcepts
        (final String term, final Integer skip, final Integer top) {
        return supplyAsync (() -> {
                try {
                    Logger.debug(getClass().getName()+".apiFindConcepts: "
                                 +"term=\""+term+"\" skip="+skip+" top="+top);
                    List<MatchedConcept> results =
                        ks.findConcepts(term, skip, top);
                    return ok (Json.toJson(results));
                }
                catch (Exception ex) {
                    Logger.error("findConcepts: "+ex.getMessage(), ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }
    
    @BodyParser.Of(value = BodyParser.AnyContent.class)
    public CompletionStage<Result> apiFindConceptsPost
        (final Integer skip, final Integer top) {
        return supplyAsync (() -> {
                try {
                    String term = request().body().asText();
                    if (term != null) {
                        Logger.debug(getClass().getName()+".apiFindConcepts: "
                                     +"term=\""+term+"\" skip="+skip
                                     +" top="+top);
                        List<MatchedConcept> results =
                            ks.findConcepts(term, skip, top);
                        return ok (Json.toJson(results));
                    }
                    return badRequest ("Invalid POST payload!");
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
                    return concept != null ? ok (Json.toJson(concept))
                        : notFound ("Can't locate concept for "+src+"/"+id);
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> apiCui (final String cui) {
        return supplyAsync (() -> {
                try {
                    Concept concept = ks.getConcept(cui);
                    return concept != null ? ok (Json.toJson(concept))
                        : notFound ("Can't locate concept for "+cui);
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

    public CompletionStage<Result> apiMetaMap (String terms) {
        return supplyAsync (() -> {
                try {
                    return ok (ks.getMetaMap().annotateAsJson(terms));
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }
    
    @BodyParser.Of(value = BodyParser.AnyContent.class)
    public CompletionStage<Result> apiMetaMapPost () {
        return supplyAsync (() -> {
                try {
                    String text = request().body().asText();
                    //Logger.debug(request().uri()+"...\n"+text);
                    if (text != null)
                        return ok (ks.getMetaMap().annotateAsJson(text));
                    return badRequest ("Invalid POST payload!");
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> apiSemRep (String text) {
        return supplyAsync (() -> {
                try {
                    byte[] xml = ks.getSemRepAsXml(text);
                    if (xml != null)
                        return ok (xml).as("application/xml");
                    return badRequest ("SemRep is not supported!");
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    @BodyParser.Of(value = BodyParser.AnyContent.class)
    public CompletionStage<Result> apiSemRepPost () {
        return supplyAsync (() -> {
                try {
                    String text = request().body().asText();
                    if (text != null) {
                        byte[] xml = ks.getSemRepAsXml(text);
                        if (xml != null)
                            return ok(xml).as("application/xml");
                        return badRequest ("SemRep is not supported!");
                    }
                    return badRequest ("Invalid POST payload!");
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }
    
    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("ksUMLSRoutes",
                    routes.javascript.Controller.apiCui(),
                    routes.javascript.Controller.apiConcept(),
                    routes.javascript.Controller.apiFindConcepts()
                    )).as("text/javascript");
    }
}

