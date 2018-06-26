package controllers.mesh;

import java.io.*;
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

import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Entry;
import blackboard.mesh.CommonDescriptor;

public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final MeshKSource ks;
    final WSClient wsclient;
    final CacheApi cache;
    final MeshDb mesh;
    
    @Inject
    public Controller (HttpExecutionContext ec, WSClient wsclient,
                       CacheApi cache, MeshKSource ks) {
        this.ks = ks;
        this.wsclient = wsclient;
        this.cache = cache;
        this.ec = ec;
        mesh = ks.getMeshDb();
    }

    static protected CompletionStage<Result> async (Result result) {
        return supplyAsync (() -> {
                return result;
            });
    }
    
    public CompletionStage<Result> search (final String q, final Integer top) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                String query = q.replaceAll("%20"," ").replaceAll("%22", "\"");
                String[] context = request().queryString().get("context");
                return ok (Json.toJson(mesh.search(query, top, context)));
            }, ec.current());
    }

    public CompletionStage<Result> mesh (final String ui) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                Entry entry = mesh.getEntry(ui);
                if (entry != null) {
                    return ok (Json.toJson(entry));
                }
                return notFound ("Unknown MeSH ui: "+ui);
            }, ec.current());
    }

    public CompletionStage<Result> parents (final String ui) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                List<Entry> parents = mesh.getParents(ui);
                if (parents != null) {
                    return ok (Json.toJson(parents));
                }
                return notFound ("Unknown MeSH ui: "+ui);
            }, ec.current());
    }

    public CompletionStage<Result> context
        (final String ui, final Integer skip, final Integer top) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                List<Entry> entries = mesh.getContext(ui, skip, top);
                if (!entries.isEmpty())
                    return ok (Json.toJson(entries));
                return notFound ("Unknown MeSH ui: "+ui);
            }, ec.current());
    }

    public CompletionStage<Result> descriptor (final String name) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                List<Entry> entries = mesh.search
                    (name.replaceAll("%20"," ").replaceAll("%22", "\""), 10);
                if (!entries.isEmpty()) {
                    CommonDescriptor desc = mesh.getDescriptor(entries.get(0));
                    if (desc != null)
                        return ok (Json.toJson(desc));
                }
                return notFound ("Can't resolve \""
                                 +name+"\" to a MeSH descriptor!");
            }, ec.current());
    }

    public Result index () {
        return ok (views.html.mesh.index.render());
    }
}
