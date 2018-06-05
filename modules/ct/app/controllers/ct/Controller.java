package controllers.ct;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;

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

public class Controller extends play.mvc.Controller {
    final WSClient wsclient;
    final CacheApi cache;
    final ClinicalTrialKSource ks;
    final ClinicalTrialDb ctdb;

    @Inject
    public Controller (WSClient wsclient, CacheApi cache,
                       ClinicalTrialKSource ks) {
        this.ks = ks;
        this.wsclient = wsclient;
        this.cache = cache;
        ctdb = ks.getClinicalTrialDb();
    }

    public Result index () {
        return ok ("Knowledge source for ClinicalTrials.gov");
    }

    public Result search (String query, int skip, int top) {
        return ok (query);
    }

    public Result resolve (String id) {
        return ok (id);
    }

    public Result conditions (Integer skip, Integer top) {
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
    }

    public Result build (Integer skip, Integer top) {
        try {
            ctdb.build(skip, top);
            return ok ("Building ClinicalTrialDb!");
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }
}
