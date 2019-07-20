package blackboard.semmed.controllers;

import java.lang.reflect.Array;
import java.util.*;
import java.io.File;
import java.util.function.Predicate;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.cache.SyncCacheApi;
import play.cache.AsyncCacheApi;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;
import play.inject.ApplicationLifecycle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.SerializationFeature;

import blackboard.pubmed.PubMedDoc;
import blackboard.semmed.Predication;
import blackboard.semmed.SemMedDbKSource;
import blackboard.umls.SemanticType;
import blackboard.mesh.MeshDb;
import blackboard.mesh.MeshKSource;
import blackboard.pubmed.index.PubMedIndexManager;
import blackboard.pubmed.index.PubMedIndex;
import static blackboard.pubmed.index.PubMedIndex.*;

public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final PubMedIndexManager pubmed;
    final AsyncCacheApi cache;
    final SemMedDbKSource semmed;
    final MeshDb mesh;
    final ObjectMapper mapper = Json.mapper();

    @Inject
    public Controller (PubMedIndexManager pubmed, SemMedDbKSource semmed,
                       MeshKSource mesh, AsyncCacheApi cache,
                       HttpExecutionContext ec) {
        this.pubmed = pubmed;
        this.ec = ec;
        this.cache = cache;
        this.semmed = semmed;
        this.mesh = mesh.getMeshDb();

        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        Logger.debug("$$" +getClass().getName()+": "+pubmed);
    }

    CompletionStage<PubMedDoc> getDoc (final Long pmid) {
        return cache.getOrElseUpdate
            (getClass().getName()+"/"+pmid, () -> {
                return pubmed.getDoc(pmid).thenApplyAsync(doc -> {
                        if (doc != null) {
                            return PubMedDoc.getInstance(doc.doc, mesh);
                        }
                        return null;
                    }, ec.current());
            });
    }

    CompletionStage<Result> getPredicationsForPMID (Long pmid)
        throws Exception {
        return getDoc(pmid).thenApplyAsync(doc -> {
                if (doc != null) {
                    try {
                        List<Predication> preds = semmed.getPredicationsByPMID
                            (doc.getPMID().toString());
                        Logger.debug("Doc "+pmid+" has "
                                     +preds.size()+" predicates!");
                        return ok (blackboard.semmed.views.html.pubmed.render
                                   (semmed, doc,
                                    preds.toArray(new Predication[0])));
                    }
                    catch (Exception ex) {
                        return internalServerError (ex.getMessage());
                    }
                }
                return notFound ("Can't find pubmed "+pmid);
            }, ec.current());
    }

    public Result cui (String cui) {
        return ok (blackboard.semmed.views.html.index.render(semmed, cui));
    }
    
    public CompletionStage<Result> pmid (final Long pmid) {
        try {
            return getPredicationsForPMID (pmid);
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve PubMed for "+pmid, ex);
            return supplyAsync (() -> internalServerError
                                ("Can't retrieve PubMed for "+pmid),
                                ec.current());
        }
    }

    Predication[] filter (final String cui, Predicate<Predication> pred) {
        try {
            return semmed.getPredications(cui).stream()
                .filter(pred)
                .toArray(Predication[]::new);
        }
        catch (Exception ex) {
            Logger.error("Predicate failed", ex);
            return new Predication[0];
        }
    }
    
    public Result index () {
        return ok (blackboard.semmed.views.html.index.render(semmed, null));
    }

    public CompletionStage<Result> predicate (final String cui,
                                              final String predicate) {
        return supplyAsync (() -> {
                Predication[] preds = filter
                    (cui, p -> p.predicate.equals(predicate));
                return ok (blackboard.semmed.views.html.predication.render
                           (semmed, cui, preds));
            }, ec.current());
    }

    public CompletionStage<Result> semtype (final String cui,
                                            final String semtype) {
        return supplyAsync (() -> {
                SemanticType st = semmed.umls.getSemanticType(semtype);
                if (st == null)
                    return ok (views.html.ui.notfound.render
                               ("Unknown semantic type <code>"
                                +semtype+"</code>!"));
                Predication[] preds =
                    filter (cui, p -> st.abbr.equals(p.subtype)
                            || st.abbr.equals(p.objtype));
                return ok (blackboard.semmed.views.html.predication.render
                           (semmed, cui, preds));
            }, ec.current());
    }
    
    public CompletionStage<Result> concept (final String sub,
                                            final String obj) {
        return supplyAsync (() -> {
                Predication[] preds = filter
                    (sub, p->obj.equalsIgnoreCase(p.objcui));
                return ok (blackboard.semmed.views.html.predication.render
                           (semmed, sub, preds));
            }, ec.current());
    }
    
    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("semmedRoutes",
                    routes.javascript.Controller.cui(),
                    routes.javascript.Controller.predicate(),
                    routes.javascript.Controller.semtype(),
                    routes.javascript.Controller.concept(),                    
                    routes.javascript.Controller.pmid()
                    )).as("text/javascript");
    }
}
