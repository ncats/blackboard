package controllers.ct;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import javax.inject.Inject;
import javax.inject.Singleton;
import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.libs.ws.WSClient;
import play.libs.ws.WSRequest;
import play.Logger;
import play.libs.Json;
import play.cache.CacheApi;
import play.libs.concurrent.HttpExecutionContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.w3c.dom.Document;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.*;
import javax.xml.transform.dom.*;

import blackboard.ct.ClinicalTrialKSource;
import blackboard.ct.Condition;
import blackboard.ct.ClinicalTrialDb;

@Singleton
public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final WSClient wsclient;
    final CacheApi cache;
    final ClinicalTrialKSource ks;
    final ClinicalTrialDb ctdb;

    @Inject
    public Controller (HttpExecutionContext ec, WSClient wsclient,
                       CacheApi cache, ClinicalTrialKSource ks) {
        this.ec = ec;
        this.ks = ks;
        this.wsclient = wsclient;
        this.cache = cache;
        ctdb = ks.getClinicalTrialDb();
    }

    public Result search (String query, int skip, int top) {
        return ok (query);
    }

    public CompletionStage<Result> resolve (final String id) {
        return supplyAsync (() -> {
                try {
                    return ok (id);
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                    return internalServerError (ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> conditions
        (final Integer skip, final Integer top) {
        return supplyAsync (() -> {
                try {
                    List<Condition> conditions = cache.getOrElse
                        ("ct/conditions/"+skip+"/"+top,
                         new Callable<List<Condition>> () {
                             public List<Condition> call () throws Exception {
                                 return ks.getClinicalTrialDb()
                                 .getConditions(skip, top);
                             }
                         });
                    return ok (Json.toJson(conditions));
                }
                catch (Exception ex) {
                    Logger.error("getAllConditions", ex);
                    return internalServerError
                        ("Can't retrieve all conditions: "+ex.getMessage());
                }
            }, ec.current());
    }

    public CompletionStage<Result> getCondition (final String name) {
        return supplyAsync (() -> {
                try {
                    Condition cond = ctdb.getCondition(name);
                    return cond != null ? ok (Json.toJson(cond))
                        : notFound ("Unknown condition: \""+name+"\"");
                }
                catch (Exception ex) {
                    return internalServerError
                        ("Can't retrieve condition: \""+name+"\"");
                }
            }, ec.current());
    }

    @BodyParser.Of(value = BodyParser.MultipartFormData.class)
    public Result initialize () {
        Http.MultipartFormData<File> body =
            request().body().asMultipartFormData();
        Http.MultipartFormData.FilePart<File> interv = body.getFile("interv");
        File file = interv.getFile();
        if (file != null) {
            try (InputStream is = new GZIPInputStream
                 (new FileInputStream (file))) {
                Logger.debug("Initializing database "+ctdb.getDbFile()+"...");
                ctdb.initialize(0, is);
            }
            catch (Exception ex) {
                Logger.error("Can't map interventions", ex);
                return internalServerError (ex.getMessage());
            }
        }
        return redirect (routes.Controller.index());
    }

    public CompletionStage<Result> index () {
        return supplyAsync (() -> {
                return ok (views.html.ct.index.render(ctdb));
            }, ec.current());
    }
}
