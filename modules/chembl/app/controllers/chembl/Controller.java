package controllers.chembl;

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

import blackboard.chembl.ChemblKSource;

@Singleton
public class Controller extends play.mvc.Controller {
    final ChemblKSource ks;
    final HttpExecutionContext ec;
    
    @Inject
    public Controller (HttpExecutionContext ec, ChemblKSource ks) {
        this.ks = ks;
        this.ec = ec;
    }

    static protected CompletionStage<Result> async (Result result) {
        return supplyAsync (() -> {
                return result;
            });
    }
    
    public Result index () {
        try {
            ks.fetchDocs();
            return ok (ks.dbver);
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public CompletionStage<Result> getActivitiesForMeshTreeNumber (String tr) {
        return supplyAsync (() -> {
                return ok ("");
            }, ec.current());
    }
}
