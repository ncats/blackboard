package blackboard.ct.controllers;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import play.mvc.*;
import play.Logger;
import play.libs.Json;
import play.cache.CacheApi;
import play.libs.concurrent.HttpExecutionContext;
import javax.inject.Inject;

import blackboard.ct.ClinicalTrialKSource;
import blackboard.ct.Condition;
import blackboard.ct.ClinicalTrialDb;
import blackboard.ct.ClinicalTrial;

public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final ClinicalTrialKSource ks;
    final ClinicalTrialDb ctdb;
    final CacheApi cache;

    @Inject
    public Controller (HttpExecutionContext ec, CacheApi cache,
                       ClinicalTrialKSource ks) {
        this.ec = ec;
        this.ks = ks;
        this.cache = cache;
        ctdb = ks.getClinicalTrialDb();
    }
    
    public CompletionStage<Result> index () {
        return supplyAsync (() -> {
                return ok (blackboard.ct.views.html.index.render(ctdb));
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
}
