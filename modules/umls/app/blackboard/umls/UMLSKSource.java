package blackboard.umls;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.regex.*;
import java.util.*;

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
import com.fasterxml.jackson.databind.JsonNode;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import akka.actor.ActorSystem;

import blackboard.*;
import play.mvc.BodyParser;
import play.mvc.Http;

import static blackboard.KEntity.*;

public class UMLSKSource implements KSource {
    public final WSClient wsclient;
    public final KSourceProvider ksp;
    private final CacheApi cache;
    private final TGT tgt;
    
    private final String APIKEY;
    private final String APIVER;

    class Request {
        final WSRequest req;
        Request (String context) throws Exception {
            req = wsclient.url(ksp.getUri()+"/"+context);
        }
        
        WSRequest q (String param, String value) {
            return req.setQueryParameter(param, value);
        }
    }

    /*
     * ticket granting ticket
     */
    class TGT {
        String url;
        TGT () throws Exception {
            url = getTGTUrl ();
        }

        String ticket () throws Exception {
            WSResponse res = wsclient.url(url)
                .setContentType("application/x-www-form-urlencoded")
                .post("service=http%3A%2F%2Fumlsks.nlm.nih.gov")
                .toCompletableFuture().get();
            
            int status = res.getStatus();
            String body = res.getBody();
            
            if (status == 201 || status == 200)
                return body;

            Logger.debug("ticket: status="+status+" body="+body);
            try {
                // get new tgt url
                url = getTGTUrl ();
                return ticket ();
            }
            catch (Exception ex) {
                Logger.error("Can't retrieve TGT url", ex);
            }
            return null;
        }
    }
    
    static Document fromInputSource (InputSource source)
        throws Exception {
        DocumentBuilderFactory factory =
            DocumentBuilderFactory.newInstance();
        factory.setFeature
            ("http://apache.org/xml/features/disallow-doctype-decl", false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        
        return builder.parse(source);
    }
    
    String getTGTUrl () throws Exception {
        WSResponse res = wsclient.url
            ("https://utslogin.nlm.nih.gov/cas/v1/api-key")
            .setFollowRedirects(true)
            .setContentType("application/x-www-form-urlencoded")
            .post("apikey="+APIKEY)
            .toCompletableFuture().get();

        String url = null;
        int status = res.getStatus();
        String body = res.getBody();                
        if (status == 201 || status == 200) {
            int pos = body.indexOf("https");
            if (pos > 0) {
                int end = body.indexOf("-cas");
                if (end > pos) {
                    url = body.substring(pos, end+4);
                }
            }
            else {
                Logger.error("Unexpected response: "+body);
            }
        }
        
        Logger.debug("+++ retrieving TGT.. "
                     +(url != null ? url : (status+" => "+body)));

        return url;
    }            
    
    
    @Inject
    public UMLSKSource (WSClient wsclient, CacheApi cache,
                        @Named("umls") KSourceProvider ksp,
                        ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;

        Map<String, String> props = ksp.getProperties();
        APIKEY = props.get("apikey");
        if (APIKEY == null)
            throw new IllegalArgumentException
                ("No UMLS \"apikey\" specified!");

        APIVER = props.containsKey("umls-version")
            ? props.get("umls-version") : "current";

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });

        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
        try {
            tgt = new TGT ();
        }
        catch (Exception ex) {
            throw new RuntimeException (ex);
        }
    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        try {
            for (KNode kn : nodes) {
                switch (kn.getType()) {
                case "query":
                    seedQuery ((String) kn.get("term"), kn, kgraph);
                    break;

                case "concept":
                    break; // 
                    
                default:
                    { String name = (String)kn.get("name");
                        if (name != null) {
                            seedQuery (name, kn, kgraph);
                        }
                        else {
                            Logger.debug(ksp.getId()
                                         +": can't resolve node of type \""
                                         +kn.getType()+"\"");
                        }
                    }
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't execute knowledge source on kgraph "
                         +kgraph.getId(), ex);            
        }
    }

    protected void seedQuery (String query, KNode kn, KGraph kg)
        throws Exception {
        WSRequest req = search (query);
        WSResponse res = req.get().toCompletableFuture().get();
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri()+": status="+res.getStatus());
        }
        else {
            JsonNode results = res.asJson().get("result").get("results");
            for (int i = 0; i < results.size(); ++i) {
                JsonNode n = results.get(i);
                Map<String, Object> props = new HashMap<>();
                props.put("cui", n.get("ui").asText());
                props.put(SOURCE_P, n.get("rootSource").asText());
                props.put(URI_P, n.get("uri").asText());
                props.put(NAME_P, n.get("name").asText());
                props.put(TYPE_P, "concept");
                KNode xn = kg.createNodeIfAbsent(props, URI_P);
                props.clear();
                props.put("value", req.getUrl()+"?string="+query);
                kg.createEdgeIfAbsent(kn, xn, "resolve", props, null);
            }
        }
    }

    public String ticket () throws Exception {
        return tgt.ticket();
    }

    public WSRequest search (String query) throws Exception {
        String ticket = tgt.ticket();
        String url = ksp.getUri()+"/search/"+APIVER;
            
        Logger.debug("++ ticket="+ticket+" query="+query);            
        return wsclient.url(url)
            .setQueryParameter("string", query)
            .setQueryParameter("ticket", ticket)
            ;
    }
}
