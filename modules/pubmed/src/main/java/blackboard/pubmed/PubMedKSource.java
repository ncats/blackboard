package blackboard.pubmed;

import java.io.StringReader;
import java.util.*;
import java.util.function.BiConsumer;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import play.Logger;
import play.Configuration;
import play.libs.ws.*;
import play.libs.Json;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import akka.actor.ActorSystem;

import com.fasterxml.jackson.databind.JsonNode;

import org.w3c.dom.Document;
import org.xml.sax.InputSource;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import blackboard.*;
import static blackboard.KEntity.*;

public class PubMedKSource implements KSource {
    private final WSClient wsclient;
    private final KSourceProvider ksp;

    @Inject
    public PubMedKSource (WSClient wsclient,
                          @Named("pubmed") KSourceProvider ksp,
                          ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;

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
        try {
            for (KNode n : nodes) {
                switch (n.getType()) {
                case "query":
                    seedQuery (kgraph, n);
                    break;
                    
                case "disease":
                    break;
                case "drug":
                    break;
                case "protein":
                    break;
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't execute knowledge source on kgraph "
                         +kgraph.getId(), ex);
        }
    }

    protected void seedQuery (KGraph kgraph, KNode node) throws Exception {
        WSRequest req = wsclient.url(ksp.getUri()+"/esearch.fcgi")
            .setFollowRedirects(true)
            .setQueryParameter("db", "pubmed")
            .setQueryParameter("retmax", "100")
            .setQueryParameter("retmode", "json")
            .setQueryParameter("term", "\"Pharmacologic Actions\"[MeSH Major Topic] AND \""+node.get("term")+"\"");
        
        WSResponse res = req.get().toCompletableFuture().get();
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri()+" returns status "+res.getStatus());
            return;
        }

        JsonNode json = res.asJson().get("esearchresult");
        if (json != null) {
            int count = json.get("count").asInt();
            JsonNode list = json.get("idlist");
            if (count > 0) {
                for (int i = 0; i < count; ++i) {
                    long pmid = list.get(i).asLong();
                    processDoc (kgraph, pmid);
                }
            }
            Logger.debug(count+" result(s) found!");        
        }
    }

    protected void processDoc (KGraph kgraph, long pmid) throws Exception {
        WSRequest req = wsclient.url(ksp.getUri()+"/efetch.fcgi")
            .setFollowRedirects(true)
            .setQueryParameter("db", "pubmed")
            .setQueryParameter("retmode", "xml")
            .setQueryParameter("id", String.valueOf(pmid));
        
        Logger.debug("fetching "+pmid); 
        WSResponse res = req.get().toCompletableFuture().get();
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri()+" returns status "+res.getStatus());
            return;
        }       

        DocumentBuilder db = DocumentBuilderFactory
            .newInstance().newDocumentBuilder();
        
        Document doc = db.parse
            (new InputSource (new StringReader (res.getBody())));
        
        //TODO: parse this document into knodes..
        NodeList nodes = doc.getElementsByTagName("PubmedArticle");
        if (nodes.getLength() == 0) {
            Logger.warn(pmid+": Not a valid PubMed DOM!");
            return;
        }

        Element article = (Element)nodes.item(0);
        nodes = article.getElementsByTagName("ArticleTitle");
        if (nodes.getLength() > 0) {
            Logger.debug(pmid+": "+nodes.item(0).getTextContent());
        }
    }
}
