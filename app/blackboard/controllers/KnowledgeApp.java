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

import blackboard.index.pubmed.PubMedIndexManager;
import blackboard.index.pubmed.PubMedIndex;
import blackboard.index.Index.FV;
import blackboard.index.Index.Facet;
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

    static String displayHtml (FV node, int min) {
        String text = node.display != null ? node.display : node.label;
        for (FV p = node.parent; p != null; p = p.parent)
            min -= 2;
        if (min < 3)
            min = 3;
        if (text.length() > min) {
            return "<span data-toggle=\"tooltip\""
                +" title=\""+text+"\" style=\"margin-right:0.5rem\">"
                +text.substring(0,min)+"...</span>";
        }
        return text;
    }
    
    public static String toHtml (FV node) {
        return toHtml (node, 20);
    }
    
    public static String toHtml (FV node, int min) {
        StringBuilder html = new StringBuilder ();
        toHtml (html, min, node);
        return html.toString();
    }
    
    public static void toHtml (StringBuilder html, int min, FV node) {
        for (FV p = node.parent; p != null; p = p.parent)
            html.append(" ");
        html.append("<li id=\""+node.getPath()+"\">"+displayHtml (node, min));
        html.append(" <span class=\"badge badge-primary badge-pill\">"
                    +node.count+"</span>");
        html.append("<ul>");
        for (FV child : node.children) {
            toHtml (html, min, child);
        }
        html.append("</ul></li>");
    }
    
    @Inject
    public KnowledgeApp (HttpExecutionContext ec, PubMedIndexManager pubmed,
                         SyncCacheApi cache, Configuration config) {
        super (ec, pubmed, cache);
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
    
    public Result _ksearch (String q, int skip, int top) throws Exception {
        SearchReferences sref = searchAndFetchReferences (q, skip, top);
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
    }
    
    public CompletionStage<Result> ksearch (String q, int skip, int top) {
        Logger.debug(">> "+request().uri());
        if ((q == null || "".equals(q))
            && null == request().getQueryString("facet")) {
            return supplyAsync (() -> {
                    return redirect (routes.KnowledgeApp.index());
                }, ec.current());
        }

        return supplyAsync (() -> {
                try {
                    return _ksearch (q, skip, top);
                }
                catch (Exception ex) {
                    Logger.error("** "+request().uri()+" failed", ex);
                    // routes.KnowledgeApp.search(q, skip, top).url()
                    return ok (views.html.ui.error.render
                               (ex.getMessage(), 500));
                }
            }, ec.current());
    }

    public Result _references (String q, int skip, int top) throws Exception {
        SearchReferences sref = searchAndFetchReferences (q, 0, 1);
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
                    return _references (q, skip, top);
                }
                catch (Exception ex) {
                    Logger.error("** "+request().uri()+" failed", ex);
                    return ok (views.html.ui.error.render
                               (ex.getMessage(), 500));
                }
            }, ec.current());
    }

    public boolean isFacetSelected (Facet facet, FV fv) {
        String[] facets = request().queryString().get("facet");
        if (facets != null) {
            for (String f : facets) {
                if (f.startsWith(facet.name)) {
                    String value = f.substring(facet.name.length()+1);
                    if (value.equals(fv.label))
                        return true;
                }
            }
        }
        return false;
    }

    public String uri (String... exclude) {
        StringBuilder uri = new StringBuilder (request().path());
        Map<String, String[]> query = new TreeMap<>(request().queryString());
        for (String p : exclude) {
            query.remove(p);
        }

        if (!query.isEmpty()) {
            int i = 0;
            for (Map.Entry<String, String[]> me : query.entrySet()) {
                for (String v : me.getValue()) {
                    if ("q".equals(me.getKey())) {
                        v = v.replaceAll("\\+", "%2B")
                            .replaceAll("'", "%27");
                    }
                    uri.append(i == 0 ? "?" : "&");
                    uri.append(me.getKey()+"="+v);
                    ++i;
                }
            }
        }
        return uri.toString();
    }
}
