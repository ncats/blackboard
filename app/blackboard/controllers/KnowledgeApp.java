package blackboard.controllers;

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
import play.libs.ws.WSResponse;
import play.Logger;
import play.Environment;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;
import play.inject.ApplicationLifecycle;

import blackboard.index.pubmed.PubMedIndexManager;
import blackboard.index.pubmed.PubMedIndex;
import static blackboard.index.pubmed.PubMedIndex.*;
import blackboard.utils.Util;

public class KnowledgeApp extends blackboard.pubmed.controllers.Controller {
    @Inject
    public KnowledgeApp (HttpExecutionContext ec, PubMedIndexManager pubmed,
                         ApplicationLifecycle lifecycle) {
        super (ec, pubmed, lifecycle);
    }

    public Result index () {
        return ok (blackboard.views.html.index.render());
    }

    public Result _search (String q, int skip, int top) throws Exception {
        SearchResult result = doSearch (q, skip, top), refs = null;
        int page = skip / top + 1;
        int[] pages = Util.paging(top, page, result.total);
        Facet facet = result.getFacet("@reference");
        if (facet != null) {
            List<Long> pmids = new ArrayList<>();
            for (int i = 0; i < Math.min(facet.size(), 10); ++i) {
                String pmid = facet.values.get(i).label;
                try {
                    pmids.add(Long.parseLong(pmid));
                }
                catch (NumberFormatException ex) {
                    Logger.warn("Bogus PMID in facet @reference: "+pmid, ex);
                }
            }
            
            if (!pmids.isEmpty()) {
                Long[] ids = {
                    28394330l,28394330l,29394237l,30857677l,30149377l,30236891l,31013637l,31026749l,30959348l,29704328l,30853207l,31028292l,31028190l,30447333l,30871211l,30688034l};
                refs = pubmed.getDocs(ids);//pmids.toArray(new Long[0]));
            }
        }
        return ok (blackboard.views.html.knowledge.render
                   (page, top, pages, result, refs));
    }
    
    public CompletionStage<Result> search (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        if (q == null || "".equals(q)) {
            return supplyAsync (() -> {
                    return redirect (routes.KnowledgeApp.index());
                }, ec.current());
        }

        return supplyAsync (() -> {
                try {
                    return _search (q, skip, top);
                }
                catch (Exception ex) {
                    Logger.error("** "+request().uri()+" failed", ex);
                    // routes.KnowledgeApp.search(q, skip, top).url()
                    return ok (views.html.ui.error.render
                               (ex.getMessage(), 500));
                }
            }, ec.current());
    }
}
