package controllers.mesh;

import java.io.*;
import java.nio.file.*;
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
import static play.mvc.Http.MultipartFormData.*;

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
import blackboard.mesh.Descriptor;
import blackboard.mesh.CommonDescriptor;

public class Controller extends play.mvc.Controller {
    final HttpExecutionContext ec;
    final MeshKSource ks;
    final WSClient wsclient;
    final CacheApi cache;
    
    @Inject
    public Controller (HttpExecutionContext ec, WSClient wsclient,
                       CacheApi cache, MeshKSource ks) {
        this.ks = ks;
        this.wsclient = wsclient;
        this.cache = cache;
        this.ec = ec;
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
                return ok (Json.toJson(ks.getMeshDb()
                                       .search(query, top, context)));
            }, ec.current());
    }

    public CompletionStage<Result> mesh (final String ui) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                Entry entry = ks.getMeshDb().getEntry(ui);
                if (entry != null) {
                    return ok (Json.toJson(entry));
                }
                return notFound ("Unknown MeSH ui: "+ui);
            }, ec.current());
    }

    public CompletionStage<Result> parents (final String ui) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                List<Entry> parents = ks.getMeshDb().getParents(ui);
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
                List<Entry> entries = ks.getMeshDb().getContext(ui, skip, top);
                if (!entries.isEmpty())
                    return ok (Json.toJson(entries));
                return notFound ("Unknown MeSH ui: "+ui);
            }, ec.current());
    }

    public CompletionStage<Result> descriptor (final String name) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                List<Entry> entries = ks.getMeshDb().search
                    (name.replaceAll("%20"," ").replaceAll("%22", "\""), 10);
                if (!entries.isEmpty()) {
                    CommonDescriptor desc = ks.getMeshDb()
                        .getDescriptor(entries.get(0));
                    if (desc != null)
                        return ok (Json.prettyPrint(Json.toJson(desc)))
                            .as("application/json");
                }
                return notFound ("Can't resolve \""
                                 +name+"\" to a MeSH descriptor!");
            }, ec.current());
    }

    public CompletionStage<Result> treeNumber (final String tr) {
        Logger.debug(">> "+request().uri());
        return supplyAsync (() -> {
                List<Descriptor> desc = ks.getMeshDb()
                    .getDescriptorsByTreeNumber(tr);
                if (!desc.isEmpty())
                    return ok (Json.prettyPrint(Json.toJson(desc)));
                return notFound ("Unknown treeNumber \""+tr+"\"!");
            }, ec.current());
    }

    public CompletionStage<Result> index () {
        return supplyAsync (() -> {
                return ok (views.html.mesh.index.render(ks));
            }, ec.current());
    }

    @BodyParser.Of(value = BodyParser.MultipartFormData.class)
    public Result buildMeshDb () {
        Http.MultipartFormData<File> body =
            request().body().asMultipartFormData();
        Http.MultipartFormData.FilePart<File> desc = body.getFile("desc");
        Http.MultipartFormData.FilePart<File> pa = body.getFile("pa");
        Http.MultipartFormData.FilePart<File> qual = body.getFile("qual");
        Http.MultipartFormData.FilePart<File> supp = body.getFile("supp");

        try {
            Path tempdir = Files.createTempDirectory("mesh");
            Logger.debug("buildMeshDb: tempdir="+tempdir);
            Files.copy(desc.getFile().toPath(),
                       tempdir.resolve(desc.getFilename()));
            Logger.debug("copied desc="+desc.getFilename()
                         +" "+desc.getFile().length());
            
            Files.copy(pa.getFile().toPath(),
                       tempdir.resolve(pa.getFilename()));
            Logger.debug("copied pa="+pa.getFilename()
                         +" "+pa.getFile().length());
            
            Files.copy(qual.getFile().toPath(),
                       tempdir.resolve(qual.getFilename()));
            Logger.debug("copied qual="+qual.getFilename()
                         +" "+qual.getFile().length());
            
            Files.copy(supp.getFile().toPath(),
                       tempdir.resolve(supp.getFilename()));
            Logger.debug("copied supp="+supp.getFilename()
                         +" "+supp.getFile().length());

            File dir = tempdir.toFile();
            dir.deleteOnExit();
            ks.initialize(dir);
        }
        catch (IOException ex) {
            Logger.error("Can't process multipart form data", ex);
            return internalServerError ("Unable to build MeSH database!");
        }

        return redirect (routes.Controller.index());
    }
}
