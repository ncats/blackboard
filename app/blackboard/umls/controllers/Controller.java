package blackboard.umls.controllers;

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

import blackboard.umls.UMLSKSource;
import blackboard.umls.Concept;
import blackboard.umls.MatchedConcept;
import blackboard.umls.DataSource;
import blackboard.umls.MetaMap;
import blackboard.umls.SemanticType;

public class Controller extends play.mvc.Controller {
    final UMLSKSource ks;
    final HttpExecutionContext ec;
    
    @Inject
    public Controller (HttpExecutionContext ec, UMLSKSource ks) {
        this.ks = ks;
        this.ec = ec;
    }

    public Result index (String query) {
        return ok (blackboard.umls.views.html.index.render(ks, query));
    }

    public CompletionStage<Result> cui (String cui) {
        return supplyAsync (() -> {
                try {
                    Concept concept = ks.getConcept(cui);
                    if (concept != null)
                        return ok (blackboard.umls.views.html.cui.render(concept, ks));
                    return index (cui);
                }
                catch (Exception ex) {
                    Logger.error("Can't retrieve concept for "+cui, ex);
                    return ok (views.html.ui.error.render
                               ("Can't retrieve concept for "+cui+": "
                                +ex.getMessage(), 500));
                }
            }, ec.current());
    }

    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("umlsRoutes",
                    routes.javascript.Controller.cui()
                    )).as("text/javascript");
    }
}
