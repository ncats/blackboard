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
    static class SearchReferences {
        SearchResult result;
        SearchResult refs;

        SearchReferences (SearchResult result, SearchResult references) {
            this.result = result;
            this.refs = references;
        }
    }
        
    @Inject
    public KnowledgeApp (HttpExecutionContext ec, PubMedIndexManager pubmed,
                         ApplicationLifecycle lifecycle) {
        super (ec, pubmed, lifecycle);
    }

    public Result index () {
        return ok (blackboard.views.html.index.render());
    }

    SearchReferences searchAndFetchReferences (String q, int skip, int top)
        throws Exception {
        SearchResult result = doSearch (q, skip, top), refs = null;
        Facet facet = result.getFacet("@reference");
        if (facet != null) {
            List<Long> pmids = new ArrayList<>();
            for (int i = 0; i < Math.min(facet.size(), 50); ++i) {
                String pmid = facet.values.get(i).label;
                try {
                    pmids.add(Long.parseLong(pmid));
                }
                catch (NumberFormatException ex) {
                    Logger.warn("Bogus PMID in facet @reference: "+pmid, ex);
                }
            }
            
            if (!pmids.isEmpty()) {
                refs = pubmed.getDocs(pmids.toArray(new Long[0]));
            }
        }
        return new SearchReferences (result, refs);
    }
    
    public Result _search (String q, int skip, int top) throws Exception {
        SearchReferences sref = searchAndFetchReferences (q, skip, top);
        int page = skip / top + 1;
        int[] pages = Util.paging(top, page, sref.result.total);
        return ok (blackboard.views.html.knowledge.render
                   (page, top, pages, sref.result, sref.refs));
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

    public CompletionStage<Result> references (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        if (q == null || "".equals(q)) {
            return supplyAsync (() -> {
                    return redirect (routes.KnowledgeApp.index());
                }, ec.current());
        }

        return supplyAsync (() -> {
                try {
                    SearchReferences sref = searchAndFetchReferences (q, 0, 1);
                    int page = skip / top + 1;
                    int[] pages = Util.paging(top, page, sref.result.size());
                    return ok (blackboard.views.html.knowledge.render
                               (page, top, pages, sref.refs, null));
                }
                catch (Exception ex) {
                    Logger.error("** "+request().uri()+" failed", ex);
                    return ok (views.html.ui.error.render
                               (ex.getMessage(), 500));
                }
            }, ec.current());
    }
}
