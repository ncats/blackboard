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

@Singleton
public class Controller extends play.mvc.Controller {
    final MeshKSource ks;
    final WSClient wsclient;
    final CacheApi cache;
    
    @Inject
    public Controller (WSClient wsclient, CacheApi cache, MeshKSource ks) {
        this.ks = ks;
        this.wsclient = wsclient;
        this.cache = cache;
    }

    public Result index () {
        return ok
            ("This is a basic implementation of the MeSH knowledge source!");
    }

    public Result search (String q) {
        return ok (q);
    }

    public Result mesh (final String ui) {
        return ok (ui);
    }
}

