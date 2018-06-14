package blackboard.umls;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.regex.*;
import java.util.*;
import java.sql.*;
import java.net.URLEncoder;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;

import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import play.db.NamedDatabase;
import play.db.Database;

import akka.actor.ActorSystem;

import blackboard.*;
import play.mvc.BodyParser;
import play.mvc.Http;

import static blackboard.KEntity.*;

public class UMLSKSource implements KSource {
    public final WSClient wsclient;
    public final KSourceProvider ksp;
    private final CacheApi cache;
    private final Database db;
    private final TGT tgt;

    private final Map<String, Set<String>> blacklist;    
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
                        @NamedDatabase("umls") Database db,
                        ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;
        this.db = db;

        Map<String, String> props = ksp.getProperties();
        APIKEY = props.get("apikey");
        if (APIKEY == null) {
            Logger.warn("No UMLS \"apikey\" specified!");
        }

        APIVER = props.containsKey("umls-version")
            ? props.get("umls-version") : "current";

        blacklist = new ConcurrentHashMap<>();
        if (ksp.getData() != null) {
            JsonNode source = ksp.getData().get("source");
            if (source != null) {
                JsonNode n = source.get("blacklist");
                Set<String> set = new HashSet<>();
                for (int i = 0; i < n.size(); ++i)
                    set.add(n.get(i).asText());
                blacklist.put("source", set);
                Logger.debug("source: "+set.size()+" blacklsit entries!");
            }
        }
        
        lifecycle.addStopHook(() -> {
                wsclient.close();
                db.shutdown();
                return F.Promise.pure(null);
            });

        TGT _tgt = null;
        try {
            _tgt = new TGT ();
        }
        catch (Exception ex) {
            Logger.warn("Can't initialize UMLS ticket granting ticket!");
        }
        tgt = _tgt;

        try (Connection con = db.getConnection()) {
            Logger.debug("Database connection ok!");
        }
        catch (SQLException ex) {
            if (tgt == null)
                throw new RuntimeException
                    ("UMLS knowledge source is unusable without due to "
                     +"neither API nor database availability!");
            Logger.warn("Can't connect to database!");
        }
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
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
                    expand (kgraph, kn);
                    break;
                    
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
                KNode xn = createConceptNodeIfAbsent (kg, n.get("ui").asText());
                Map<String, Object> props = new HashMap<>();
                props.put("value", req.getUrl()+"?string="+query);
                kg.createEdgeIfAbsent(kn, xn, "resolve", props, null);
            }
        }
    }

    public KNode createConceptNodeIfAbsent (KGraph kg, String cui)
        throws Exception {
        JsonNode n = getCui (cui);
        if (n == null)
            return null;
            
        Map<String, Object> props = new HashMap<>();
        props.put("cui", n.get("ui").asText());
        String uri = n.get("atoms").asText();
        int pos = uri.lastIndexOf('/');
        if (pos > 0) {
            uri = uri.substring(0, pos);
        }
        
        props.put(URI_P, uri);
        props.put(NAME_P, n.get("name").asText());
        props.put(TYPE_P, "concept");
        List<String> types = new ArrayList<>();
        Set<String> semtypes = new TreeSet<>();
        JsonNode sn = n.get("semanticTypes");        
        for (int i = 0; i < sn.size(); ++i) {
            String t = sn.get(i).get("uri").asText();
            pos = t.lastIndexOf('/');
            if (pos > 0)
                t = t.substring(pos+1);
            types.add(t);
            semtypes.add(sn.get(i).get("name").asText());
        }
        props.put("semtypes", types.toArray(new String[0]));
        KNode kn = kg.createNodeIfAbsent(props, URI_P);
        for (String t : kn.getTags())
            semtypes.remove(t);

        if (!semtypes.isEmpty())
            kn.addTag(semtypes.toArray(new String[0]));
        
        return kn;
    }

    public Map<String, String> getRelatedCuis (String cui) throws Exception {
        JsonNode json = getContent (cui, "relations");
        Map<String, String> relations = new HashMap<>();
        if (json != null) {
            for (int i = 0; i < json.size(); ++i) {
                JsonNode n = json.get(i);
                if (!n.get("obsolete").asBoolean()) {
                    String rel = n.get("relationLabel").asText();
                    String uri = n.get("relatedId").asText();
                    String ui = n.get("ui").asText();
                    int pos = uri.lastIndexOf('/');
                    if (pos > 0)
                        uri = uri.substring(pos+1);
                    relations.put(ui, rel+":"+uri);
                }
            }
        }
        return relations;
    }

    public List<KEdge> expand (KGraph kg, KNode node, String... types)
        throws Exception {
        String cui = (String) node.get("cui");
        if (cui == null)
            throw new IllegalArgumentException
                ("Node doesn't have CUI defined!");
        Set<String> _types = new HashSet<>();
        for (String t : types)
            _types.add(t.toUpperCase());
        
        List<KEdge> edges = new ArrayList<>();
        Map<String, String> relations = getRelatedCuis (cui);
        for (Map.Entry<String, String> me : relations.entrySet()) {
            String[] toks = me.getValue().split(":");
            if (_types.isEmpty() || _types.contains(toks[0])) {
                KNode n = createConceptNodeIfAbsent (kg, toks[1]);
                KEdge edge = kg.createEdgeIfAbsent(node, n, me.getKey());
                edge.put("scope", toks[0]);
                edges.add(edge);
            }
        }
        return edges;
    }

    public String ticket () throws Exception {
        if (tgt == null)
            throw new RuntimeException ("No APIKEY configured!");
        return tgt.ticket();
    }

    public WSRequest search (String query) throws Exception {
        String ticket = ticket ();
        String url = ksp.getUri()+"/search/"+APIVER;
            
        Logger.debug("++ ticket="+ticket+" query="+query);
        return wsclient.url(url)
            .setQueryParameter("string", query.replaceAll("%20", "+"))
            .setQueryParameter("ticket", ticket)
            ;
    }

    public WSRequest cui (String cui) throws Exception {
        String ticket = ticket ();
        String url = ksp.getUri()+"/content/"+APIVER+"/CUI/"+cui;
        Logger.debug("++ CUI: ticket="+ticket+" cui="+cui);
        return wsclient.url(url).setQueryParameter("ticket", ticket);
    }

    public WSRequest content (String cui, String context) throws Exception {
        String ticket = ticket ();
        String url = ksp.getUri()+"/content/"+APIVER+"/CUI/"+cui+"/"+context;
        Logger.debug("++ "+context+": ticket="+ticket+" cui="+cui);
        return wsclient.url(url).setQueryParameter("ticket", ticket);
    }

    public WSRequest source (String src, String id, String context)
        throws Exception {
        String ticket = ticket ();
        String url = ksp.getUri()+"/content/"+APIVER+"/source/"+src+"/"+id;
        if (context != null)
            url += "/"+context;
        Logger.debug("++ "+src+": ticket="+ticket
                     +" id="+id+" context="+context);
        return wsclient.url(url).setQueryParameter("ticket", ticket);
    }

    public JsonNode getCui (final String cui) throws Exception {
        return cache.getOrElse("umls/"+cui, new Callable<JsonNode> () {
                public JsonNode call () throws Exception {
                    WSResponse res =
                        cui(cui).get().toCompletableFuture().get();
                    try {
                        if (res.getStatus() == 200) {
                            return res.asJson().get("result");
                        }
                    }
                    catch (Exception ex) {
                        Logger.error("Can't retrieve CUI "+cui+" ==> "
                                     +res.getBody(), ex);
                    }
                    Logger.warn("** Can't retrieve CUI "+cui);
                    return null;
                }
            });
    }

    public JsonNode getSource (final String src, final String id,
                               final String context)
        throws Exception {
        return cache.getOrElse
            ("umls/"+src+"/"+id+(context!=null?context:""),
             new Callable<JsonNode> () {
                 public JsonNode call () throws Exception {
                     WSResponse res = source(src, id, context)
                         .get().toCompletableFuture().get();
                     return res.getStatus() == 200
                         ? res.asJson().get("result") : null;
                 }
             });
    }

    public JsonNode getContent (final String cui, final String context)
        throws Exception {
        return cache.getOrElse
            ("umls/"+context+"/"+cui, new Callable<JsonNode>() {
                    public JsonNode call () throws Exception {
                        WSResponse res = content(cui, context)
                            .get().toCompletableFuture().get();
                        return 200 == res.getStatus()
                            ? res.asJson().get("result") : null;
                    }
                });
    }

    public JsonNode getSearch (final String query,
                               final int skip, final int top) throws Exception {
        return cache.getOrElse
            ("umls/search/"+query+"/"+top+"/"+skip, new Callable<JsonNode>() {
                    public JsonNode call () throws Exception {
                        WSResponse res = search(query)
                            .setQueryParameter("pageSize", String.valueOf(top))
                            .setQueryParameter("pageNumber",
                                               String.valueOf(skip/top+1))
                            .get().toCompletableFuture().get();
                        if (200 == res.getStatus()) {
                            return res.asJson().get("result").get("results");
                        }
                        return null;
                    }
                });
    }

    public List<MatchedConcept> findConcepts (String term) throws Exception {
        return findConcepts (term, 0, 10);
    }
    
    public List<MatchedConcept> findConcepts (String term, int skip, int top)
        throws Exception {
        List<MatchedConcept> matches = new ArrayList<>();
        try (Connection con = db.getConnection();
             // resolve term to cui
             PreparedStatement pstm1 = con.prepareStatement
             ("select distinct cui from MRCONSO where str=?");
             // this assumes the following sql is run on the MRCONSO table:
             // alter table MRCONSO add FULLTEXT INDEX X_STR_FULLTEXT (STR);
             PreparedStatement pstm2 = con.prepareStatement
             ("select cui,str,sab,match(str) "
              +"against (? in natural language mode) "
              +"as score from MRCONSO where match(str) against "
              +"(? in natural language mode) and stt='PF' "
              +"and lat='ENG' and ispref='Y' "
              +"order by score desc limit ? offset ?")) {
            pstm1.setString(1, term);
            ResultSet rset = pstm1.executeQuery();
            List<String> cuis = new ArrayList<>();
            while (rset.next()) {
                String cui = rset.getString(1);
                cuis.add(cui);
            }
            rset.close();

            if (cuis.isEmpty()) {
                Logger.warn("No exact concept matching \""+term
                            +"\"; trying fulltext search...");
                try {
                    pstm2.setString(1, term);
                    pstm2.setString(2, term);
                    pstm2.setInt(3, top);
                    pstm2.setInt(4, skip);
                    
                    rset = pstm2.executeQuery();
                    while (rset.next()) {
                        String cui = rset.getString("CUI");
                        String str = rset.getString("STR");
                        String sab = rset.getString("SAB");
                        float score = rset.getFloat("SCORE");
                        Logger.debug(cui+" "+String.format("%1$.2f", score)
                                     +" "+sab+" "+str);
                        Concept concept = getConcept (cui);
                        if (concept != null) {
                            matches.add(new MatchedConcept
                                        (concept, cui, str, score));
                        }
                    }
                    rset.close();
                }
                catch (SQLException ex) {
                    Logger.error("Can't execute fulltext search on \""+term
                                 +"\"!", ex);
                }
            }
            else {
                // now identify the canonical cui
                if (cuis.size() > 1) {
                    Logger.warn("Term \""+term+"\" matches "
                                +cuis.size()+" concepts..."+cuis);
                }

                for (String c : cuis) {
                    Concept cui = getConcept (c);
                    if (cui != null)
                        matches.add(new MatchedConcept (cui));
                }
            }
        }
        
        return matches;
    }

    public Concept getConcept (final String cui) throws Exception {
        return cache.getOrElse("umls/"+cui, new Callable<Concept> () {
                public Concept call () throws Exception {
                    return _getConcept (cui);
                }
            });
    }
    
    public Concept _getConcept (String cui) throws Exception {
        Concept concept = null;
        Set<String> source = blacklist.get("source");        
        try (Connection con = db.getConnection();
             // get canonical name
             PreparedStatement pstm1 = con.prepareStatement
             ("select * from MRCONSO where lat='ENG' and cui = ?");
             // definition
             PreparedStatement pstm2 = con.prepareStatement
             ("select * from MRDEF where cui = ?");
             // semantic types
             PreparedStatement pstm3 = con.prepareStatement
             ("select * from MRSTY where cui = ?");
             PreparedStatement pstm4 = con.prepareStatement
             ("select * from MRREL where cui1 = ?")) {
            pstm1.setString(1, cui);
            ResultSet rset = pstm1.executeQuery();
            List<Synonym> synonyms = new ArrayList<>();
            while (rset.next()) {
                String stt = rset.getString("STT");
                String ispref = rset.getString("ISPREF");
                String ts = rset.getString("TS");
                String name = rset.getString("STR");
                String sab = rset.getString("SAB");
                String scui = rset.getString("SCUI");
                String sdui = rset.getString("SDUI");
                if (source == null || !source.contains(sab)) {
                    Source src = new Source (sab, scui, sdui);
                    if ("PF".equals(stt)
                        && "Y".equals(ispref) && "P".equals(ts)) {
                        concept = new Concept (cui, name, src);
                    }
                    else { // synonym
                        synonyms.add(new Synonym (name, src));
                    }
                }
            }
            rset.close();
            
            if (concept != null) {
                concept.synonyms.addAll(synonyms);
                // definitions
                pstm2.setString(1, cui);
                rset = pstm2.executeQuery();
                while (rset.next()) {
                    String def = rset.getString("DEF");
                    String sab = rset.getString("SAB");
                    if (source == null || !source.contains(sab)) {
                        concept.definitions.add(new Definition (def, sab));
                    }
                }
                rset.close();
                
                // semantic types
                pstm3.setString(1, cui);
                rset = pstm3.executeQuery();
                while (rset.next()) {
                    String id = rset.getString("TUI");
                    String type = rset.getString("STY");
                    concept.semanticTypes.add(new SemanticType (id, type));
                }
                rset.close();

                // relations
                pstm4.setString(1, cui);
                rset = pstm4.executeQuery();
                while (rset.next()) {
                    String rui = rset.getString("RUI");
                    String ui = rset.getString("CUI2");
                    String rel = rset.getString("REL");
                    String rela = rset.getString("RELA");
                    String sab = rset.getString("SAB");
                    if (source == null || !source.contains(sab)) {
                        concept.relations.add
                            (new Relation (rui, ui, rel, rela, sab));
                    }
                }
                rset.close();
            }
            else {
                Logger.error
                    ("Concept \""+cui+"\" has no canonical name!");
            }
        }
        return concept;
    }

    public Concept getConcept (final String src, final String id)
        throws Exception {
        return cache.getOrElse("umls/"+src+"/"+id, new Callable<Concept> () {
                public Concept call () throws Exception {
                    return _getConcept (src, id);
                }
            });
    }
    
    public Concept _getConcept (final String src, final String id)
        throws Exception {
        Concept cui = null;
        try (Connection con = db.getConnection()) {
            switch (src) {
            case "cui":
                cui = getConcept (id);
                break;
                
            case "lui":
            case "sui":
            case "aui":
            case "scui":
            case "sdui":
                try (PreparedStatement pstm = con.prepareStatement
                     ("select distinct cui from MRCONSO where "+src+" = ?")) {
                    pstm.setString(1, id);
                    ResultSet rset = pstm.executeQuery();
                    if (rset.next()) {
                        cui = getConcept (rset.getString(1));
                    }
                    else {
                        Logger.warn("No concept mapping for "+src+"="+id);
                    }
                    rset.close();
                }
                break;

            default:
                Logger.warn("Unknown source: "+src);
            }
        }
        return cui;
    }

    public List<DataSource> getDataSources () throws Exception {
        return cache.getOrElse
            ("umls/datasources", new Callable<List<DataSource>> () {
                    public List<DataSource> call () throws Exception {
                        return _getDataSources ();
                    }
                });
    }
    
    public List<DataSource> _getDataSources () throws Exception {
        List<DataSource> datasources = new ArrayList<>();
        try (Connection con = db.getConnection();
             Statement stm = con.createStatement()) {
            ResultSet rset = stm.executeQuery("select * from MRSAB");
            while (rset.next()) {
                DataSource ds = new DataSource
                    (rset.getString("RSAB"), rset.getString("VSAB"),
                     rset.getString("SON"));
                datasources.add(ds);
            }
            rset.close();
        }
        return datasources;
    }
}
