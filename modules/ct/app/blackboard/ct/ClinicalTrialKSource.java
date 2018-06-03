package blackboard.ct;

import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.regex.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.inject.Named;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import akka.actor.ActorSystem;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import blackboard.*;
import play.mvc.BodyParser;

import static blackboard.KEntity.*;

import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;
import blackboard.mesh.Entry;
import blackboard.mesh.Concept;
import blackboard.mesh.Term;

import blackboard.umls.UMLSKSource;

public class ClinicalTrialKSource implements KSource {
    static final String ALL_CONDITIONS =
        "https://clinicaltrials.gov/ct2/search/browse?brwse=cond_alpha_all";
    
    final WSClient wsclient;
    final KSourceProvider ksp;
    final CacheApi cache;
    final MeshDb mesh;
    final UMLSKSource umls;

    @Inject
    public ClinicalTrialKSource (WSClient wsclient, CacheApi cache,
                                 @Named("ct") KSourceProvider ksp,
                                 MeshKSource meshKS, UMLSKSource umls,
                                 ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;
        this.mesh = meshKS.getMeshDb();
        this.umls = umls;

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
    }

    public List<Condition> getAllConditions () throws Exception {
        WSRequest req = wsclient.url(ALL_CONDITIONS)
            .setFollowRedirects(true);
        Logger.debug("+++ retrieving all conditions..."+req.getUrl());
        
        List<Condition> conditions = new ArrayList<>();
        WSResponse res = req.get().toCompletableFuture().get();
        Logger.debug("+++ parsing..."+res.getUri());
        if (200 == res.getStatus()) {
            BufferedReader br = new BufferedReader
                (new InputStreamReader (res.getBodyAsStream()));
            Pattern p = Pattern.compile("\"([^\"]+)");
            for (String line; (line = br.readLine()) != null; ) {
                int start = line.indexOf('[');
                if (start > 0) {
                    int end = line.indexOf(']', start+1);
                    if (end > start) {
                        line = line.substring(start+1, end);
                        start = line.indexOf("\">");
                        if (start > 0) {
                            end = line.indexOf("</a>", start);
                            if (end > start) {
                                String name = line.substring(start+2, end);
                                Matcher m = p.matcher(line.substring(end+5));
                                if (m.find()) {
                                    int count = Integer.parseInt
                                        (m.group(1).trim().replaceAll(",",""));
                                    Condition cond = new Condition (name);
                                    cond.count = count;
                                    resolveMesh (cond);
                                    resolveUMLS (cond);
                                    Logger.debug
                                        ("#### \""+cond.name+"\" mesh="
                                         +cond.meshUI
                                         +" umls="+cond.umlsUI);

                                    conditions.add(cond);
                                }
                            }
                        }
                    }
                }
            }
        }
        
        return conditions;
    }

    void resolveMesh (Condition cond) {
        List<Entry> results = mesh.search("\""+cond.name+"\"", 10);
        Entry matched = null;
        for (Entry r : results) {
            if (r instanceof Term) {
                List<Entry> entries = mesh.getContext(r.ui, 0, 10);
                for (Entry e : entries) {
                    if (e instanceof Concept) {
                        matched = e;
                        break;
                    }
                }
                
                if (matched != null)
                    break;
            }
            else if (r instanceof Concept) {
                matched = r;
                break;
            }
        }
        
        if (matched != null) {
            cond.meshUI = matched.ui;
            Logger.debug("\""+cond.name+"\" => "+matched.ui
                         +" \""+matched.name+"\"");
        }
    }

    void resolveUMLS (Condition cond) {
        try {
            if (cond.meshUI != null) {
                WSResponse res = umls.source
                    ("MSH", cond.meshUI, "atoms/preferred")
                    .get().toCompletableFuture().get();
                if (200 == res.getStatus()) {
                    JsonNode json = res.asJson();
                    if (json.has("result")) {
                        json = json.get("result");
                        if (json.has("concept")) {
                            String url = json.get("concept").asText();
                            int pos = url.lastIndexOf('/');
                            if (pos > 0)
                                cond.umlsUI = url.substring(pos+1);
                        }
                    }
                }
                else {
                    Logger.error("UMLS source service returns status: "
                                 +res.getStatus());
                }
            }
            else {
                WSResponse res = umls.search(cond.name)
                    .get().toCompletableFuture().get();
                if (200 == res.getStatus()) {
                    JsonNode json = res.asJson();
                    if (json.has("result")) {
                        json = json.get("result").get("results").get(0);
                        if (json.has("ui")) {
                            cond.umlsUI = json.get("ui").asText();
                            Logger.debug("\""+cond.name+"\" => "+cond.umlsUI
                                         +" \""+json.get("name").asText()
                                         +"\"");
                        }
                    }
                }
                else {
                    Logger.error("UMLS search service returns status: "
                                 +res.getStatus());                    
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't match UMLS for condition "+cond.name, ex);
        }
    }
}
