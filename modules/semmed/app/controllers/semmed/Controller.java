package controllers.semmed;

import java.io.File;
import java.util.*;
import java.util.function.Predicate;
import java.util.concurrent.CompletionStage;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.inject.Provider;

import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.Environment;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import blackboard.semmed.SemMedDbKSource;
import blackboard.semmed.Predication;
import blackboard.semmed.PredicateSummary;
import blackboard.semmed.SemanticType;
import blackboard.umls.MatchedConcept;
import blackboard.umls.Concept;
import blackboard.pubmed.PubMedDoc;

@Singleton
public class Controller extends play.mvc.Controller {
    final SemMedDbKSource ks;
    final HttpExecutionContext ec;
    final Environment env;
    
    @Inject
    public Controller (HttpExecutionContext ec,
                       Environment env, SemMedDbKSource ks) {
        this.ks = ks;
        this.ec = ec;
        this.env = env;
    }

    public Result index (String query) {
        return cui (query);
    }

    public Result cui (String cui) {
        return ok (views.html.semmed.index.render(ks, cui));
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

    public CompletionStage<Result> apiSearch
        (final String q, final Integer skip, final Integer top) {
        return supplyAsync (() -> {
                try {
                    Logger.debug(getClass().getName()+".apiSearch: "
                                 +"q=\""+q+"\" skip="+skip+" top="+top);
                    
                    List<MatchedConcept> concepts =
                        ks.umls.findConcepts(q, skip, top);
                    ArrayNode result = Json.newArray();
                    for (MatchedConcept mc : concepts) {
                        final PredicateSummary ps =
                            ks.getPredicateSummary(mc.cui);

                        ObjectNode concept = Json.newObject();
                        if (mc.score != null)
                            concept.put("score", mc.score);
                        concept.put("cui", mc.cui);
                        concept.put("name", mc.name);
                        concept.put("source", mc.concept.source.name);
                        concept.put("semtypes",
                                    Json.toJson(mc.concept.semanticTypes
                                                .stream().map(t -> t.name)
                                                .toArray(String[]::new)));
                        ObjectNode obj = Json.newObject();
                        obj.put("concept", concept);
                        obj.put("predicates", Json.toJson(ps.predicates));
                        obj.put("semtypes", Json.toJson(ps.semtypes));
                        ArrayNode ary = Json.newArray();
                        Set<String> order = new TreeSet<>((a,b) -> {
                                int d = ps.concepts.get(b) - ps.concepts.get(a);
                                if (d == 0)
                                    d = a.compareTo(b);
                                return d;
                            });
                        order.addAll(ps.concepts.keySet());
                        for (String k : order) {
                            Concept c = ks.umls.getConcept(k);
                            if (c != null) {
                                ObjectNode oc = Json.newObject();
                                oc.put("cui", k);
                                oc.put("count", ps.concepts.get(k));
                                oc.put("name", c.name);
                                oc.put("semtypes", Json.toJson
                                       (c.semanticTypes.stream()
                                        .map(t -> t.name)
                                        .toArray(String[]::new)));
                                ary.add(oc);
                            }
                        }
                        obj.put("concepts", ary);
                        obj.put("pmids", Json.toJson(ps.pmids));
                        result.add(obj);
                    }
                    return ok (result);
                }
                catch (Exception ex) {
                    Logger.error("Search failed", ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> apiPredicateSummary (final String cui) {
        return supplyAsync (() -> {
                try {
                    PredicateSummary ps = ks.getPredicateSummary(cui);
                    return ok (Json.prettyPrint(Json.toJson(ps)))
                        .as("application/json");
                }
                catch (Exception ex) {
                    Logger.error("PredicateSummary failed", ex);
                    return internalServerError (ex.getMessage());
                }                    
            }, ec.current());
    }

    Predication[] filter (final String cui, Predicate<Predication> pred) {
        try {
            return ks.getPredications(cui).stream()
                .filter(pred)
                .toArray(Predication[]::new);
        }
        catch (Exception ex) {
            Logger.error("Predicate failed", ex);
            return new Predication[0];
        }
    }
  
    public CompletionStage<Result> apiPredicate (final String cui,
                                                 final String predicate) {
        return supplyAsync (() -> {
                Predication[] preds =
                    filter (cui, p -> p.predicate.equals(predicate));
                return ok (Json.toJson(preds));
            }, ec.current());
    }

    public CompletionStage<Result> apiSemtype (final String cui,
                                               final String semtype) {
        return supplyAsync (() -> {
                Predication[] preds =
                    filter (cui, p -> semtype.equals(p.subtype)
                            || semtype.equals(p.objtype));
                return ok (Json.toJson(preds));
            }, ec.current());
    }

    public CompletionStage<Result> apiPMID (final Long pmid) {
        return supplyAsync (() -> {
                try {
                    return ok (Json.toJson(ks.getPredicationsByPMID
                                           (pmid.toString())));
                }
                catch (Exception ex) {
                    Logger.error("Can't fetch predication for pmid "+pmid, ex);
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> predicate (final String cui,
                                              final String predicate) {
        return supplyAsync (() -> {
                Predication[] preds = filter
                    (cui, p -> p.predicate.equals(predicate));
                return ok (views.html.semmed.predication.render
                           (ks, cui, preds));
            }, ec.current());
    }

    public CompletionStage<Result> semtype (final String cui,
                                            final String semtype) {
        return supplyAsync (() -> {
                SemanticType st = ks.getSemanticType(semtype);
                if (st == null)
                    return ok (views.html.core.notfound.render
                               ("Unknown semantic type <code>"
                                +semtype+"</code>!"));
                Predication[] preds =
                    filter (cui, p -> st.abbr.equals(p.subtype)
                            || st.abbr.equals(p.objtype));
                return ok (views.html.semmed.predication.render
                           (ks, cui, preds));
            }, ec.current());
    }

    public CompletionStage<Result> concept (final String sub,
                                            final String obj) {
        return supplyAsync (() -> {
                Predication[] preds = filter
                    (sub, p->obj.equalsIgnoreCase(p.object));
                return ok (views.html.semmed.predication.render
                           (ks, sub, preds));
            }, ec.current());
    }

    public CompletionStage<Result> pubmed (final Long pmid) {
        return supplyAsync (() -> {
                try {
                    PubMedDoc doc = ks.pubmed.getPubMed(pmid.toString());
                    List<Predication> preds =
                        ks.getPredicationsByPMID(doc.getPMID().toString());
                    Logger.debug("Doc "+pmid+" has "
                                 +preds.size()+" predicates!");
                    return ok (views.html.semmed.pubmed.render
                               (ks, doc, preds.toArray(new Predication[0])));
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve PubMed for "+pmid, ex);
                    return internalServerError
                        ("Can't retrieve PubMed for "+pmid);
                }
            }, ec.current());
    }
    
    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("semmedRoutes",
                    routes.javascript.Controller.cui(),
                    routes.javascript.Controller.predicate(),
                    routes.javascript.Controller.semtype(),
                    routes.javascript.Controller.concept(),                    
                    routes.javascript.Controller.pubmed(),
                    routes.javascript.Controller.apiSemanticTypeLookup(),
                    routes.javascript.Controller.apiSearch()
                    )).as("text/javascript");
    }
}
