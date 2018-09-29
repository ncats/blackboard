package controllers.pubmed;

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

import blackboard.pubmed.PubMedKSource;
import blackboard.pubmed.PubMedDoc;

@Singleton
public class Controller extends play.mvc.Controller {
    final PubMedKSource ks;
    final WSClient wsclient;
    final CacheApi cache;
    
    @Inject
    public Controller (Configuration config, WSClient wsclient,
                       CacheApi cache, PubMedKSource ks) {
        this.ks = ks;
        this.wsclient = wsclient;
        this.cache = cache;
    }

    public Result index () {
        return ok
            ("This is a basic implementation of the PubMed knowledge source!");
    }

    public Result mesh (String id) {
        try {
            WSRequest req = wsclient.url
                ("https://id.nlm.nih.gov/mesh/"+id+".json")
                .setFollowRedirects(true);
            Logger.debug("+++ resolving..."+req.getUrl());
            WSResponse res = req.get().toCompletableFuture().get();
            return status (res.getStatus(), res.asJson());
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve MeSH "+id, ex);
            return internalServerError
                ("Can't retrieve MeSH "+id+"; "+ex.getMessage());
        }
    }

    public Result searchMeSH (String q) {
        try {
            PubMedKSource.MeSH[] mesh = ks.searchMeSH(q);
            return ok (Json.toJson(mesh));
        }
        catch (Exception ex) {
            Logger.error("Can't search MeSH: "+q, ex);
            return internalServerError
                ("Can't search MeSH \""+q+"\": "+ex.getMessage());
        }
    }

    public Result search (String q) {
        return ok (q);
    }

    public Result pmid (final Long pmid, final String format) {
        try {
            return cache.getOrElse
                ("pubmed/"+pmid+"."+format, new Callable<Result> () {
                        public Result call () throws Exception {
                            return "xml".equalsIgnoreCase(format)
                                ? getPubMedXml (pmid) : getPubMedJson (pmid);
                        }
                    });
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve PubMed "+pmid, ex);
            return internalServerError ("Can't retrieve PubMed "
                                        +pmid+": "+ex.getMessage());
        }
    }

    Result getPubMedXml (Long pmid) throws Exception {
        Document doc = ks.getDocument(pmid.toString());
        if (doc == null)
            return notFound ("Can't retrieve PubMed article "+pmid);
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream ();
        Transformer tf = TransformerFactory.newInstance().newTransformer();
        tf.transform(new DOMSource (doc), new StreamResult (baos));
        return ok(baos.toByteArray()).as("application/xml");
    }

    Result getPubMedJson (Long pmid) throws Exception {
        PubMedDoc doc = ks.getPubMedDoc(pmid.toString());
        if (doc == null)
            return notFound ("Can't retrieve PubMed article "+pmid);

        return ok (Json.toJson(doc));
    }
}
