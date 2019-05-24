package blackboard.umls;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.inject.Named;
import java.util.regex.*;
import java.util.*;
import java.sql.*;
import java.io.*;
import java.net.URLEncoder;
import java.util.concurrent.CompletableFuture;
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
import play.Environment;

import akka.actor.ActorSystem;

import blackboard.*;
import play.mvc.BodyParser;
import play.mvc.Http;

import static blackboard.KEntity.*;

@Singleton
public class UMLSKSource implements KSource {
    public final KSourceProvider ksp;

    private final WSClient wsclient;
    private final SyncCacheApi cache;
    private final Database db;
    private final MetaMap metamap;
    private final String SEMREP_URL;
    public final List<SemanticType> semanticTypes;
    
    final Pattern cuiregex = Pattern.compile("^[cC]\\d+");    
    private final Map<String, Set<String>> blacklist;

    @Inject
    public UMLSKSource (WSClient wsclient, SyncCacheApi cache,
                        Environment env,
                        @Named("umls") KSourceProvider ksp,
                        @NamedDatabase("umls") Database db,
                        ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;
        this.db = db;

        Map<String, String> props = ksp.getProperties();

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

        if (props.containsKey("metamap.host")) {
            String host = props.get("metamap.host");
            if (props.containsKey("metamap.port")) {
                metamap = new MetaMap
                    (host, Integer.parseInt(props.get("metamap.port")));
            }
            else {
                metamap = new MetaMap (host);
            }
            metamap.setCache(cache);
        }
        else {
            metamap = null;
        }

        if (metamap == null)
            Logger.warn("*** METAMAP server is not available! ***");

        if (props.containsKey("semrep.url")) {
            SEMREP_URL = props.get("semrep.url");
        }
        else {
            SEMREP_URL = null;
        }

        semanticTypes = new ArrayList<>();
        String semtype = props.get("semtypes");
        if (semtype == null)
            Logger.warn(ksp.getName()
                        +": No semantic-types property specified!");
        else {
            try (BufferedReader br = new BufferedReader
                 (new InputStreamReader (env.resourceAsStream(semtype)))) {
                for (String line; (line = br.readLine()) != null; ) {
                    String[] toks = line.split("\\|");
                    if (toks.length == 3) {
                        semanticTypes.add(new SemanticType
                                          (toks[1], toks[0], toks[2]));
                    }
                }
                Logger.debug(semanticTypes.size()+" semantic types loaded!");
            }
            catch (IOException ex) {
                Logger.error("Can't parse semanticTypes: "+semtype, ex);
            }
        }
        
        lifecycle.addStopHook(() -> {
                wsclient.close();
                db.shutdown();
                return CompletableFuture.completedFuture(null);
            });

        try (Connection con = db.getConnection()) {
            Logger.debug("Database connection ok!");
        }
        catch (SQLException ex) {
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
        List<MatchedConcept> concepts = new ArrayList();
        concepts = findConcepts(query);
        if(concepts==null||concepts.isEmpty()) {
            Logger.warn("UMLS query on "+query+" failed");
        }
        else {
            for (MatchedConcept concept : concepts) {
                String cui = concept.concept.cui;
                Logger.debug("CUI="+cui);
                Map<String,Object> props = new HashMap<>();
                props.put("name",concept.name);
                //Hitting DB for concepts instead of API here.  Revert to createConceptNodeIfAbsent if API is desired
                KNode xn = createConceptNodeIfAbsentDb(kg,cui);

                kg.createEdgeIfAbsent(kn,xn,"resolve",props,null);
            }
        }
    }

    public KNode createConceptNodeIfAbsent (KGraph kg, String cui)
        throws Exception {
        Concept concept = getConcept (cui);
        if (concept == null)
            return null;

        Map<String, Object> props = new HashMap<>();
        props.put("cui", concept.cui);
        props.put(URI_P, "http://purl.obolibrary.org/obo/UMLS_"+concept.cui);
        props.put(NAME_P, concept.name);
        props.put(TYPE_P, "concept");
        Set<String> semtypes = new TreeSet<>();
        for (SemanticType st : concept.semanticTypes)
            semtypes.add(st.name);
        props.put("semtypes", semtypes.toArray(new String[0]));
        KNode kn = kg.createNodeIfAbsent(props, URI_P);
        for (String t : kn.getTags())
            semtypes.remove(t);

        if (!semtypes.isEmpty())
            kn.addTag(semtypes.toArray(new String[0]));
        
        return kn;
    }
    
    public KNode createConceptNodeIfAbsentDb( KGraph kg, String cui)
            throws Exception {
        Concept concept = getConcept(cui);
        Map<String, Object> props = new HashMap<>();
        List<String> types = new ArrayList<>();
        Set<String> semtypes = new TreeSet<>();
        props.put("cui",concept.cui);
        props.put(NAME_P,concept.name);
        props.put(TYPE_P,"concept");
        List<SemanticType> st = concept.semanticTypes;
        st.forEach(type->{
            Logger.debug("TYPE+TYPE.NAME "+type.id+" "+type.name.toString());
            types.add(type.id);
            semtypes.add(type.name.toString());
        });
        props.put("semtypes", types.toArray(new String[0]));
        KNode kn = kg.createNodeIfAbsent(props, URI_P);
        for (String t : kn.getTags())
            semtypes.remove(t);

        if (!semtypes.isEmpty())
            kn.addTag(semtypes.toArray(new String[0]));

        return kn;
    }

    public Map<String, String> getRelatedCuis (String cui) throws Exception {
        Map<String, String> relations = new HashMap<>();
        Concept concept = getConcept (cui);
        if (concept != null) {
            for (Relation rel : concept.relations) {
                String ui = rel.cui;
                relations.put(ui, rel.type+":"+concept.cui);
            }
        }
        return relations;
    }

    public Database getDatabase () {return db;}
    
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

    public List<MatchedConcept> findConcepts (String term) throws Exception {
        return findConcepts (term, 0, 10);
    }
    
    public List<MatchedConcept> findConcepts (String term, int skip, int top)
        throws Exception {
        List<MatchedConcept> matches = new ArrayList<>();

        term = term.trim();
        if ("".equals(term))
            return matches;

        Matcher m = cuiregex.matcher(term);
        boolean iscui = m.matches();
        Logger.debug(getClass().getName()+".findConcepts: term=\""+term
                     +"\" skip="+skip+" top="+top+" iscui="+iscui);
        if (iscui) {
            matches.add(new MatchedConcept (getConcept (term)));
            return matches;
        }
        
        try (Connection con = db.getConnection();
             // resolve term to cui
             PreparedStatement pstm1 = con.prepareStatement
             ("select distinct cui from MRCONSO where str=?");
             // this assumes the following sql is run on the MRCONSO table:
             // alter table MRCONSO add FULLTEXT INDEX X_STR_FULLTEXT (STR);
             PreparedStatement pstm2 = con.prepareStatement
             ("select cui,str,sab,match(str) "
              +"against (? in natural language mode) / length(str) "
              +"as score from MRCONSO where match(str) against "
              +"(? in natural language mode) and stt='PF' "
              +"and lat='ENG' and ispref='Y' and ts='P' "
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
        return cache.getOrElseUpdate("umls/"+cui, new Callable<Concept> () {
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
                    SemanticType st = getSemanticType (id);
                    if (st != null)
                        concept.semanticTypes.add(st);
                    else
                        Logger.warn("Unknown semantic type: "+id);
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
        return cache.getOrElseUpdate
            ("umls/"+src+"/"+id, new Callable<Concept> () {
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
        return cache.getOrElseUpdate
            ("umls/datasources", new Callable<List<DataSource>> () {
                    public List<DataSource> call () throws Exception {
                        return _getDataSources ();
                    }
                });
    }

    public List<DataSource> getDataSources (String name) throws Exception {
        List<DataSource> sources = new ArrayList<>();
        for (DataSource ds : getDataSources ()) {
            if (name.equalsIgnoreCase(ds.name))
                sources.add(ds);
        }
        return sources;
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

    public MetaMap getMetaMap () { return metamap; }
    public byte[] getSemRepAsXml (String text) throws Exception {
        if (SEMREP_URL == null)
            return null;
        WSResponse res = wsclient.url(SEMREP_URL)
            .setContentType("text/plain")
            .post(text)
            .toCompletableFuture().get();
        int status = res.getStatus();
        Logger.debug(status+": "+SEMREP_URL);
        if (status == 200 || status == 201) {
            return res.asByteArray();
        }
        Logger.error("status="+status+" body="+res.getBody());
        return null;
    }

    public SemanticType getSemanticType (String str) {
        for (SemanticType st : semanticTypes) {
            if (st.abbr.equalsIgnoreCase(str) || st.id.equalsIgnoreCase(str))
                return st;
        }
        return null;
    }
}
