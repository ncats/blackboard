package blackboard.controllers;

import java.lang.reflect.Array;
import java.util.*;
import java.io.File;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import play.Configuration;
import play.mvc.*;

import play.Logger;
import play.Environment;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;
import play.inject.ApplicationLifecycle;
import play.cache.SyncCacheApi;
import play.cache.AsyncCacheApi;

import blackboard.pubmed.index.PubMedIndexManager;
import blackboard.pubmed.index.PubMedIndex;
import blackboard.index.Index.FV;
import blackboard.index.Index.Facet;
import static blackboard.pubmed.index.PubMedIndex.*;
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
                         AsyncCacheApi cache, Configuration config) {
        super (ec, pubmed, cache);
    }

    public Result index () {
        return ok (blackboard.views.html.index.render());
    }
    
    CompletionStage<SearchReferences> searchAndFetchReferences
        (String q, int skip, int top) throws Exception {
        return doSearch (q, skip, top).thenApplyAsync(result -> {
                Facet facet = result.getFacet("@reference");
                List<Long> pmids = new ArrayList<>();                
                if (facet != null) {
                    for (int i = 0; i < Math.min(facet.size(), 50); ++i) {
                        String pmid = facet.values.get(i).label;
                        try {
                            pmids.add(Long.parseLong(pmid));
                        }
                        catch (NumberFormatException ex) {
                            Logger.warn
                                ("Bogus PMID in facet @reference: "+pmid, ex);
                        }
                    }
                }

                SearchResult srefs = null;
                if (!pmids.isEmpty()) {
                    try {
                        CompletionStage<SearchResult> stage =
                            pubmed.getDocs(pmids.toArray(new Long[0]));
                        srefs = stage.toCompletableFuture().get();
                    }
                    catch (Exception ex) {
                        Logger.error("failed to retrieve references!", ex);
                    }
                }
                return new SearchReferences (result, srefs);
            }, ec.current());
    }
    
    public CompletionStage<Result> _ksearch (String q, int skip, int top)
        throws Exception {
        return searchAndFetchReferences (q, skip, top).thenApplyAsync(sref -> {
                if (skip > sref.result.total)
                    return redirect (routes.KnowledgeApp.ksearch(q, 0, top));
        
                int page = skip / top + 1;
                int[] pages = Util.paging(top, page, sref.result.total);
                Map<Integer, String> urls = new TreeMap<>();
                for (int i = 0; i < pages.length; ++i) {
                    Call call = routes.KnowledgeApp.ksearch
                        (q, (pages[i]-1)*top, top);
                    urls.put(pages[i], Util.getURL(call, request ()));
                }

                return ok (blackboard.views.html.knowledge.render
                           (this, page, pages, urls, sref.result, sref.refs));
            }, ec.current());
    }
    
    public CompletionStage<Result> ksearch (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        if ((q == null || "".equals(q))
            && null == request().getQueryString("facet")) {
            return supplyAsync (() -> {
                    return redirect (routes.KnowledgeApp.index());
                }, ec.current());
        }
        
        try {
            return  _ksearch (q, skip, top);
        }
        catch (Exception ex) {
            Logger.error("search failed: "+q, ex);
            return supplyAsync (() -> ok (views.html.ui.error.render
                                          (ex.getMessage(), 500)),
                                ec.current());
        }
    }

    public CompletionStage<Result> _references (String q, int skip, int top)
        throws Exception {
        return searchAndFetchReferences(q, 0, 1).thenApplyAsync(sref -> {
                if (skip > sref.refs.size())
                    return redirect (routes.KnowledgeApp.references(q, 0, top));
        
                int page = skip / top + 1;
                int[] pages = Util.paging(top, page, sref.refs.size());
                Map<Integer, String> urls = new TreeMap<>();
                for (int i = 0; i < pages.length; ++i) {
                    Call call = routes.KnowledgeApp.references
                        (q, (pages[i]-1)*top, top);
                    urls.put(pages[i], Util.getURL(call, request ()));
                }
                
                return ok (blackboard.views.html.knowledge.render
                           (KnowledgeApp.this, page, pages, urls,
                            sref.refs.page(skip, top), null));
            }, ec.current());
    }
    
    public CompletionStage<Result> references (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        if (q == null || "".equals(q)) {
            return supplyAsync (() -> {
                    return redirect (routes.KnowledgeApp.index());
                }, ec.current());
        }

        try {
            return _references (q, skip, top);
        }
        catch (Exception ex) {
            Logger.error("** "+request().uri()+" failed", ex);
            return supplyAsync (() -> ok (views.html.ui.error.render
                                          (ex.getMessage(), 500)),
                                ec.current());
        }
    }
}
