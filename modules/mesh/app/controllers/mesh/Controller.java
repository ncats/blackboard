package controllers.mesh;

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

import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Entry;

public class Controller extends play.mvc.Controller {
    final MeshKSource ks;
    final WSClient wsclient;
    final CacheApi cache;
    final MeshDb mesh;
    
    @Inject
    public Controller (WSClient wsclient, CacheApi cache, MeshKSource ks) {
        this.ks = ks;
        this.wsclient = wsclient;
        this.cache = cache;
        mesh = ks.getMeshDb();
    }

    public Result index () {
        return ok
            ("This is a basic implementation of the MeSH knowledge source!");
    }

    public Result search (String q, Integer top) {
        Logger.debug(">> "+request().uri());
        q = q.replaceAll("%20"," ").replaceAll("%22", "\"");
        String[] context = request().queryString().get("context");
        return ok (Json.toJson(mesh.search(q, top, context)));
    }

    public Result mesh (final String ui) {
        Logger.debug(">> "+request().uri());        
        Entry entry = mesh.getEntry(ui);
        if (entry != null) {
            return ok (Json.toJson(entry));
        }
        return notFound ("Unknown MeSH ui: "+ui);
    }

    public Result parents (final String ui) {
        Logger.debug(">> "+request().uri());
        List<Entry> parents = mesh.getParents(ui);
        if (parents != null) {
            return ok (Json.toJson(parents));
        }
        return notFound ("Unknown MeSH ui: "+ui);
    }

    public Result context (final String ui, Integer skip, Integer top) {
        Logger.debug(">> "+request().uri());
        List<Entry> entries = mesh.getContext(ui, skip, top);
        if (!entries.isEmpty())
            return ok (Json.toJson(entries));
        return notFound ("Unknown MeSH ui: "+ui);
    }
}
