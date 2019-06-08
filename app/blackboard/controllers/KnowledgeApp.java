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
        SearchResult result = doSearch (q, skip, top);
        int npages = (result.total+(top-1)) / top;
        int page = skip / top + 1;
        return ok (blackboard.views.html.knowledge.render
                   (page, npages, result));
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
