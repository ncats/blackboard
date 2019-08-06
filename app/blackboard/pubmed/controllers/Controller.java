package blackboard.pubmed.controllers;

import java.lang.reflect.Array;
import java.util.*;
import java.io.File;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.stream.Collectors;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import play.Configuration;
import play.mvc.*;
import play.libs.ws.WSResponse;
import play.Logger;
import play.Environment;
import play.mvc.BodyParser;
import play.libs.Json;
import play.libs.concurrent.HttpExecutionContext;
import play.routing.JavaScriptReverseRouter;
import play.cache.SyncCacheApi;
import play.cache.AsyncCacheApi;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.commons.lang3.StringUtils;

import blackboard.pubmed.index.PubMedIndexManager;
import blackboard.pubmed.index.PubMedIndex;
import static blackboard.pubmed.index.PubMedIndex.*;
import static blackboard.index.Index.Facet;
import static blackboard.index.Index.FV;
import static blackboard.index.Index.TermVector;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.MeshDb;
import blackboard.semmed.SemMedDbKSource;
import blackboard.pubmed.PubMedDoc;
import blackboard.utils.Util;

public class Controller extends play.mvc.Controller {
    public static final JsonNode EMPTY_JSON = Json.newObject();
    final protected HttpExecutionContext ec;
    final protected ObjectMapper mapper = Json.mapper();
    final protected AsyncCacheApi cache;
    
    final public PubMedIndexManager pubmed;
    @Inject public SemMedDbKSource semmed;
    @Inject public MeshKSource mesh;

    @Inject
    public Controller (HttpExecutionContext ec, PubMedIndexManager pubmed,
                       AsyncCacheApi cache) {
        this.pubmed = pubmed;
        this.ec = ec;
        this.cache = cache;

        mapper.configure(SerializationFeature.WRITE_EMPTY_JSON_ARRAYS, false);
        Logger.debug("$$" +getClass().getName()+": "+pubmed);
    }
    
    public Result index () {
        return ok (blackboard.pubmed.views.html.mock.index.render());
    }

    public Http.Request req () {
        return Http.Context.current().request();
    }

    public String getQueryString (String param) {
        return request().getQueryString(param);
    }

    public Map<String, String[]> queryString () {
        return request().queryString();
    }
    
    public String[] queryString (String param) {
        return queryString().get(param);
    }

    Map<String, Object> parseFacets () {
        Map<String, Object> facets = null;
        String[] params = queryString ("facet");
        if (params != null) {
            facets = new TreeMap<>();
            for (String p : params) {
                int pos = p.indexOf('/');
                if (pos > 0) {
                    String name = p.substring(0, pos);
                    Object value = p.substring(pos+1);
                    if (name.equals(FACET_GRANTAGENCY)
                        || name.startsWith(FACET_TR)) {
                        value = ((String)value).split("\\.");
                    }
                    
                    Object old = facets.get(name);
                    if (old != null) {
                        if (old instanceof String[]) {
                            int len = Array.getLength(old);
                            if (value instanceof String[]) {
                                Object[] values = new Object[]{
                                    old, value
                                };
                                facets.put(name, values);
                            }
                            else {
                                String[] values = new String[len+1];
                                for (int i = 0; i < len; ++i)
                                    values[i] = (String)Array.get(old, i);
                                values[len] = (String) value;
                                facets.put(name, values);
                            }
                        }
                        else if (old instanceof Object[]) {
                            int len = Array.getLength(old);
                            Object[] values = new Object[len+1];
                            for (int i = 0; i < len; ++i)
                                values[i] = Array.get(old, i);
                            values[len] = value;
                            facets.put(name, values);
                        }
                        else {
                            if (value instanceof String[]) {
                                facets.put(name, new Object[]{
                                        old, value
                                    });
                            }
                            else {
                                facets.put(name, new String[]{
                                        (String)old,
                                        (String)value
                                    });
                            }
                        }
                    }
                    else {
                        facets.put(name, value);
                    }
                }
                else {
                    Logger.warn("Not a valid facet: "+p);
                }
            }
        }
        return facets;
    }

    JsonNode toJson (Map<String, Object> fmap) {
        /*
        ObjectNode json = Json.newObject();
        for (Map.Entry<String, Object> me : fmap.entrySet()) {
            Object value = me.getValue();
            if (value instanceof Object[]) {
                Object[] values = (Object[])value;
                List<String> vs = new ArrayList<>();
                for (Object v : values) {
                    if (v instanceof String[]) {
                        vs.add(StringUtils.join((String[])v, '.'));
                    }
                    else
                        vs.add((String)v);
                }
                json.put(me.getKey(), mapper.valueToTree(vs));
            }
            else {
                json.put(me.getKey(), mapper.valueToTree(value));
            }
        }
        return json;
        */
        return mapper.valueToTree(fmap);
    }

    public CompletionStage<JsonNode>
        _filter (Map<String, Object> facets, int skip, int top) {
        return pubmed.search(facets, skip, top).thenApplyAsync
            (result -> {
                //Logger.debug("Cache missed: "+key);
                ObjectNode json = Json.newObject();
                json.put("query", mapper.valueToTree(result.query));
                json.put("count", result.size());
                json.put("total", result.total);
                ObjectNode content =
                    (ObjectNode)mapper.valueToTree(result);
                content.remove("query");
                content.remove("count");
                content.remove("total");
                json.put("content", content);
                return json;
            }, ec.current());
    }

    public CompletionStage<Result> filter (int skip, int top) {
        Logger.debug(">> "+req().uri());
        try {
            Map<String, Object> facets = parseFacets ();
            if (facets == null || facets.isEmpty())
                return supplyAsync (() -> badRequest ("No facets specified!"),
                                    ec.current());
            
            return _filter (facets, skip, top)
                .thenApplyAsync(json -> {
                        String path = getQueryString ("path");
                        if (path != null)
                            json = json.at(path);
                        return ok (json.isMissingNode() ? EMPTY_JSON : json);
                    }, ec.current());
        }
        catch (Exception ex) {
            Logger.error("Search failed", ex);
            return supplyAsync (() -> internalServerError
                                ("Internal server error: "+ex.getMessage()),
                                ec.current());
        }
    }

    public CompletionStage<SearchResult> doSearch (String q, int skip, int top)
        throws Exception {
        Map<String, Object> facets = parseFacets ();
        int slop = 1;
        try {
            String p = getQueryString ("slop");
            if (p != null)
                slop = Integer.parseInt(p);
        }
        catch (NumberFormatException ex) {
            Logger.error("Bogus slop parameter", ex);
        }
                    
        return pubmed.search(getQueryString ("field"), q,
                             facets, skip, top, slop);
    }
    
    public CompletionStage<JsonNode> _search
        (String q, int skip, int top) throws Exception {
        return doSearch (q, skip, top).thenApplyAsync(result -> {
                ObjectNode query =
                    (ObjectNode) mapper.valueToTree(result.query);
                query.put("rewrite", result.query.rewrite().toString());
                ObjectNode json = Json.newObject();                    
                json.put("query", query);
                json.put("count", result.size());
                json.put("total", result.total);
                
                ObjectNode content = (ObjectNode)mapper.valueToTree(result);
                content.remove("query");
                content.remove("count");
                content.remove("total");
                json.put("content", content);
                
                return json;
            }, ec.current());
    }
    
    public CompletionStage<Result> search (String q, int skip, int top) {
        Logger.debug(">> "+req().uri());
        try {
            return _search(q, skip, top).thenApplyAsync(json -> {
                    String path = getQueryString ("path");
                    if (path != null) {
                        json = json.at(path);
                    }
                    return ok (json.isMissingNode() ? EMPTY_JSON : json);
                }, ec.current());
        }
        catch (Exception ex) {
            Logger.error("Search failed", ex);
            return supplyAsync (() -> internalServerError
                                ("Internal server error: "+ex.getMessage()),
                                ec.current());
        }
    }

    public CompletionStage<Result> facets (int fdim) {
        Logger.debug(">> "+req().uri());        
        try {
            Map<String, Object> facets = parseFacets ();
            CompletionStage<SearchResult> result
                = facets == null || facets.isEmpty()
                ? pubmed.facets(fdim) : pubmed.facets(facets);
            
            return result.thenApplyAsync(r -> {
                    ObjectNode json = Json.newObject();
                    json.put("count", 0);
                    json.put("total", r.total);
                    ObjectNode content = (ObjectNode)mapper.valueToTree(r);
                    content.remove("query");
                    content.remove("count");
                    content.remove("total");
                    json.put("content", content);
                    String path = getQueryString ("path");
                    JsonNode n = json;
                    if (path != null)
                        n = n.at(path);
                    return ok (n.isMissingNode() ? EMPTY_JSON : n);
                }, ec.current());
        }
        catch (Exception ex) {
            Logger.error("Search failed", ex);
            return supplyAsync (() -> internalServerError
                                ("Internal server error: "+ex.getMessage()),
                                ec.current());
        }
    }
    
    public CompletionStage<Result> pmid (Long pmid, String format) {
        Logger.debug(">> "+req().uri());        
        try {
            CompletionStage<MatchedDoc> result = pubmed.getDoc(pmid);
            return result.thenApplyAsync(doc -> {
                    if (doc != null)
                        return ok(doc.toXmlString()).as("application/xml");
                    return notFound ("Can't find PMID "+pmid);
                }, ec.current());
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve doc: "+pmid, ex);
            return supplyAsync (() -> internalServerError
                                ("Internal server error: "+ex.getMessage()));
        }
    }

    CompletionStage<SearchResult> getReferences (MatchedDoc mdoc) {
        final String key = getClass()+"/"+mdoc.pmid+"/references";
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                PubMedDoc doc = mdoc.toDoc();
                List<Long> pmids = doc.references.stream()
                    .map(r -> r.pmids[0]).collect(Collectors.toList());
                Logger.debug("REFERENCES "+doc.pmid+"]: "+pmids);
                return pubmed.getDocs(pmids.toArray(new Long[0]));
            });
    }
    
    public CompletionStage<Result> _field (Long pmid, String field) {
        try {
            CompletionStage<MatchedDoc> result = pubmed.getDoc(pmid);
            return result.thenApplyAsync(doc -> {
                    if (doc != null) {
                        ObjectNode obj = Json.newObject();
                        switch (field) {
                        case "title":
                            obj.put(field, doc.title);
                            break;
                            
                        case "abstract":
                            obj.put(field, mapper.valueToTree(doc.abstracts));
                            break;
                            
                        case "references":
                            try {
                                SearchResult r = getReferences(doc)
                                    .toCompletableFuture().get();
                                for (MatchedDoc d : r.docs) {
                                    obj.put(String.valueOf(d.pmid),
                                            d.title);
                                }
                            }
                            catch (Exception ex) {
                                return internalServerError (ex.getMessage());
                            }
                            break;
                            
                        case "year":
                            obj.put("year", doc.year);
                            break;

                        case "journal":
                            obj.put("journal", doc.journal);
                            break;
                            
                        default:
                            return notFound ("Unsupported field: "+field);
                        }
                        
                        //Logger.debug(field+"["+pmid+"]"+ obj);
                        return ok (obj);
                    }
                    return notFound ("Can't find PMID "+pmid);
                }, ec.current());
        }
        catch (Exception ex) {
            Logger.error("Can't retrieve doc: "+pmid, ex);
            return supplyAsync (()->internalServerError
                                ("Internal server error: "+ex.getMessage()));
        }
    }
    
    public CompletionStage<Result> field (Long pmid, String field) {
        Logger.debug(">> "+req().uri());
        final String key = getClass()+"/field/"+pmid+"/"+field;        
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                return _field (pmid, field);
            });
    }

    public CompletionStage<Result> article (Long pmid) {
        final String q = getQueryString ("q");
        final boolean hasFacets = getQueryString ("facet") != null;
        final String uri = req().uri();
        try {
            return pubmed.getDoc(pmid).thenApplyAsync(doc -> {
                    if (doc != null) {
                        PubMedDoc pmdoc = PubMedDoc.getInstance
                            (doc.doc, mesh.getMeshDb());
                        try {
                            CompletionStage<SearchResult> result =
                                q != null || hasFacets
                                ? doSearch (q, 0, 1) : pubmed.facets();
                            SearchResult r =
                                result.toCompletableFuture().get().clone();

                            String[] treeNumbers = pmdoc.getTreeNumbers();
                            for (Facet f : r.getFacets()) {
                                if (f.name.startsWith(FACET_TR))
                                    f.filter(treeNumbers);
                            }
                            for (PubMedDoc.Grant g : pmdoc.grants) {
                                Logger.debug("$$ "+g.id+" "+g.type);
                            }
                            return ok (blackboard.pubmed.views.html
                                       .article.render(Controller.this,
                                                       r, pmdoc));
                        }
                        catch (Exception ex) {
                            Logger.error("** "+uri+" failed", ex);
                            return internalServerError (ex.getMessage());
                        }
                    }
            
                    return ok(blackboard.views.html.error.render
                              ("Unknown PubMed article: "+pmid, 400));
                }, ec.current());
        }
        catch (Exception ex) {
            Logger.error("** "+uri+" failed", ex);
            return supplyAsync (() -> ok (views.html.ui.error.render
                                          (ex.getMessage(), 500)),
                                ec.current());
        }
    }
    
    @BodyParser.Of(value = BodyParser.Text.class)
    public CompletionStage<Result> batch (String format) {
        List<Long> pmids = new ArrayList<>();
        for (String t : req().body().asText().split("[,;\\s]+")) {
            try {
                pmids.add(Long.parseLong(t));
            }
            catch (NumberFormatException ex) {
                Logger.warn("Bogus PMID: "+t);
            }
        }
        try {
            CompletionStage<SearchResult> result =
                pubmed.getDocs(pmids.toArray(new Long[0]));
            return result.thenApplyAsync
                (r -> {
                    try {
                        return ok(r.exportXML()).as("application/xml");
                    }
                    catch (Exception ex) {
                        return internalServerError (ex.getMessage());
                    }
                }, ec.current());
        }
        catch (Exception ex) {
            return supplyAsync (() -> internalServerError
                                ("Internal server error: "+ex.getMessage()));
        }
    }

    public boolean isFacetSelected (Facet facet, FV fv) {
        String[] facets = queryString ("facet");
        if (facets != null) {
            for (String f : facets) {
                if (f.startsWith(facet.name)) {
                    String value = f.substring(facet.name.length()+1);
                    if (value.equals(fv.label))
                        return true;
                }
            }
        }
        return false;
    }

    public String uri (String... exclude) {
        StringBuilder uri = new StringBuilder (req().path());
        Map<String, String[]> query = new TreeMap<>(queryString ());
        for (String p : exclude) {
            query.remove(p);
        }

        if (!query.isEmpty()) {
            int i = 0;
            for (Map.Entry<String, String[]> me : query.entrySet()) {
                for (String v : me.getValue()) {
                    if ("q".equals(me.getKey())) {
                        v = v.replaceAll("\\+", "%2B")
                            .replaceAll("'", "%27");
                    }
                    uri.append(i == 0 ? "?" : "&");
                    uri.append(me.getKey()+"="+v);
                    ++i;
                }
            }
        }
        return uri.toString();
    }

    static String displayHtml (FV node, int min) {
        String text = node.display != null ? node.display : node.label;
        for (FV p = node.parent; p != null; p = p.parent)
            min -= 2;
        if (min < 3)
            min = 3;
        if (text.length() > min) {
            return "<span data-toggle=\"tooltip\""
                +" title=\""+text+"\" style=\"margin-right:0.5rem\">"
                +text.substring(0,min)+"...</span>";
        }
        return text;
    }
    
    public static String toHtml (FV node) {
        return toHtml (node, 20);
    }
    
    public static String toHtml (FV node, int min) {
        StringBuilder html = new StringBuilder ();
        toHtml (html, min, node);
        return html.toString();
    }
    
    public static void toHtml (StringBuilder html, int min, FV node) {
        for (FV p = node.parent; p != null; p = p.parent)
            html.append(" ");
        html.append("<li id=\""+node.getPath()+"\" class=\"pubmed-facetnode\""
                    +" data-facetcount=\""+node.count+"\"");
        if (node.specified) {
            html.append(" data-jstree='{\"icon\":\"fa fa-check\"}'");
        }
        html.append(">"+displayHtml (node, min));
        html.append(" <span class=\"badge badge-primary badge-pill\">"
                    +Util.format(node.count)+"</span>");
        html.append("<ul>");
        for (FV child : node.children) {
            toHtml (html, min, child);
        }
        html.append("</ul></li>");
    }

    /*
     * FIXME: facets doesn't yet work!
     */
    public CompletionStage<Result> termVector (String field, String q) {
        try {
            SearchQuery query = null;
            Map<String, Object> facets = parseFacets ();
            if (q != null || (facets != null && !facets.isEmpty()))
                query = new TextQuery (q, facets);
            CompletionStage<TermVector> result =
                pubmed.getTermVector(field, query);
            return result.thenApplyAsync
                (tv -> ok (Json.toJson(tv)), ec.current());
        }
        catch (Exception ex) {
            Logger.error("failed to retrieve term vector for "+field, ex);
            return supplyAsync (() -> internalServerError
                                ("Internal server error: "+ex.getMessage()));
        }
    }
    
    public Result jsRoutes () {
        return ok (JavaScriptReverseRouter.create
                   ("pubmedRoutes",
                    routes.javascript.Controller.search(),
                    routes.javascript.Controller.facets(),
                    routes.javascript.Controller.pmid(),
                    routes.javascript.Controller.field(),
                    routes.javascript.Controller.termVector()
                    )).as("text/javascript");
    }
}
