package blackboard.pubmed;

import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.joda.time.DateTime;
import org.json.JSONObject;
import org.json.JSONWriter;
import play.Logger;
import play.libs.Json;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import play.cache.*;
import akka.actor.ActorSystem;
import org.json.XML;

import com.fasterxml.jackson.databind.JsonNode;

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

public class PubMedKSource implements KSource {
    private final WSClient wsclient;
    private final KSourceProvider ksp;
    private final CacheApi cache;
    private final String[] blacklist;
    private final String[] whitelist;
    
    private final String EUTILS_BASE;
    private final String MESH_BASE;
    private final Integer MAX_RESULTS;
    
    interface Resolver {
        void resolve (JsonNode json, KNode kn, KGraph kg);
    }

    class MeSH implements Comparable<MeSH> {
        public final String ui;
        public String name;
        public final List<String> treeNumbers = new ArrayList<>();

        MeSH (String ui) {
            this.ui = ui;
        }

        public int hashCode () { return ui.hashCode(); }
        public boolean equals (Object o) {
            if (o instanceof MeSH) {
                return ui.equals(((MeSH)o).ui);
            }
            return false;
        }
        public int compareTo (MeSH m) { return ui.compareTo(m.ui); }
        public Map<String, Object> encode (Map<String, Object> props) {
            props.put(NAME_P, name);
            props.put(TYPE_P, "mesh");
            props.put("ui", ui);
            props.put(URI_P, MESH_BASE+"/"+ui);
            props.put("treeNumbers", treeNumbers.toArray(new String[0]));
            return props;
        }
    }
    
    //static ActorSystem system = ActorSystem.create("WSClient");

    
    @Inject
    public PubMedKSource (WSClient wsclient, CacheApi cache,
                          @Named("pubmed") KSourceProvider ksp,
                          ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;

        Map<String, String> props = ksp.getProperties();
        EUTILS_BASE = props.get("uri");
        MESH_BASE = props.get("mesh");
        MAX_RESULTS = Integer.parseInt((String)props.get("max-results"));

        if (EUTILS_BASE == null)
            throw new IllegalArgumentException
                (ksp.getId()+" doesn't have \"uri\" property defined!");
        if (MESH_BASE == null)
            throw new IllegalArgumentException
                (ksp.getId()+" doesn't have \"mesh\" property defined!");

        List<String> wlist = new ArrayList<>();
        List<String> blist = new ArrayList<>();
        if (ksp.getData() != null) {
            JsonNode mesh = ksp.getData().get("mesh");
            if (mesh != null) {
                JsonNode n = mesh.get("blacklist");
                if (n != null) {
                    for (int i = 0; i < n.size(); ++i)
                        blist.add(n.get(i).asText());
                    Logger.debug(blist.size()+" blacklist entries!");
                }
                n = mesh.get("whitelist");
                if (n != null) {
                    for (int i = 0; i < n.size(); ++i)
                        wlist.add(n.get(i).asText());
                    Logger.debug(wlist.size()+" whitelist entries!");
                }
            }
        }
        this.whitelist = wlist.toArray(new String[0]);
        this.blacklist = blist.toArray(new String[0]);

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
            for (KNode kn : nodes) {
                switch (kn.getType()) {
                case "query":
                    seedQuery ((String)kn.get("term"), kn, kgraph);
                    break;
                    
                default:
                    { String query = (String)kn.get("name");
                        if (query != null)
                            seedQuery (query, kn, kgraph);
                        else
                            Logger.warn("Can't expand node "+kn.getId()
                                        +"; type="+kn.getType());
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
        // resolve through mesh
        MeSH[] meshes = queryMeSH (query);
        for (MeSH m : meshes) {
            Map<String, Object> props = m.encode(new TreeMap<>());
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                kg.createEdgeIfAbsent(kn, xn, m.ui);
            }
        }

        // resolve through pubmed
        String url = ksp.getUri() + "/esearch.fcgi";
        Map<String, String> q = new HashMap<>();
        q.put("db", "pubmed");
        q.put("retmax", String.valueOf(MAX_RESULTS));
        q.put("retmode", "json");
        q.put("sort","relevance");
        q.put("term", query);
        resolve(ksp.getUri() + "/esearch.fcgi",
                q, kn, kg, this::resolveGeneric);
    }

    protected void seedMeSH (KNode kn, KGraph kg) throws Exception {
    }

    protected void resolveGeneric (JsonNode json, KNode kn, KGraph kg) {
        try {
            JsonNode esearchresult = json.get("esearchresult");
            JsonNode idList = esearchresult.get("idlist");
            //seedDrug (idList,kn,kg);
            //seedGene (idList,kn,kg);
            resolvePubmed (idList, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve pubmed", ex);
        }
    }
    
    protected void resolvePubmed (JsonNode idList, KNode kn, KGraph kg)
        throws Exception {
        for(int i = 0; i < Math.min(MAX_RESULTS, idList.size()); ++i) {
            resolvePubmed (idList.get(i).asText(), kn, kg);
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

    KNode instrumentDoc (Document doc, KGraph kg) throws Exception {
        NodeList nodes = doc.getElementsByTagName("PMID");
        if (nodes.getLength() < 1) 
            throw new IllegalArgumentException ("Not a valid PubMed XML!");
        String pmid = ((Element)nodes.item(0)).getTextContent();

        String title = "";
        String type = "article";
        nodes = doc.getElementsByTagName("ArticleTitle");
        if (nodes.getLength() > 0) 
            title = ((Element)nodes.item(0)).getTextContent();
        else {
            // perhaps book?
            nodes = doc.getElementsByTagName("BookTitle");
            if (nodes.getLength() > 0) {
                title = ((Element)nodes.item(0)).getTextContent();
                type = "book";
            }
        }
        
        Map<MeSH, String[]> meshes = new HashMap<>();
        nodes = doc.getElementsByTagName("MeshHeading");
        if (nodes != null) {
            for (int j = 0; j < nodes.getLength(); j++) {
                //add if-has element
                Element element = (Element) nodes.item(j);
                NodeList headings =
                    element.getElementsByTagName("DescriptorName");
                if (headings.getLength() > 0) {
                    Element heading = (Element) headings.item(0);
                    final String meshId = heading.getAttribute("UI");
                    
                    String[] treeNums = cache.getOrElse
                        (meshId, new Callable<String[]> () {
                                public String[] call () throws Exception {
                                    return getTreeNumbers (meshId);
                                }
                            });
                    
                    if (treeNums != null && treeNums.length > 0) {
                        MeSH mesh = new MeSH (meshId);
                        mesh.name = heading.getTextContent();
                        for (String n : treeNums)
                            mesh.treeNumbers.add(n);
                        
                        NodeList qn = element.getElementsByTagName
                            ("QualifierName");
                        Set<String> qualifiers = new HashSet<>();
                        for (int k = 0; k < qn.getLength(); ++k) {
                            Element elm = (Element) qn.item(0);
                            qualifiers.add(elm.getTextContent());
                        }
                        
                        meshes.put(mesh, qualifiers.toArray(new String[0]));
                    }
                    else {
                        Logger.debug(" ** ignore "+meshId+" \""
                                     +heading.getTextContent()+"\"");
                    }
                }
                else {
                    Logger.warn(" ** "+pmid
                                +": MeshHeading has no DescriptorName **");
                }
            }
        }
        
        Logger.debug("+++++ instrumenting pubmed..."
                     +pmid+": "+title+" mesh="+nodes.getLength()
                     +"/"+meshes.size());

        KNode dn = null;
        if (!meshes.isEmpty()) {
            Map<String, Object> props = new TreeMap<>();
            props.put("pmid", pmid);
            props.put(TYPE_P, type);
            props.put(URI_P, "https://ncbi.nlm.nih.gov/pubmed/"+pmid);
            props.put(NAME_P, title);
            
            nodes = doc.getElementsByTagName("Journal");
            if (nodes.getLength() > 0) {
                Element journal = (Element)nodes.item(0);
                nodes = journal.getElementsByTagName("Title");
                props.put("journal", ((Element)nodes.item(0)).getTextContent());
                nodes = journal.getElementsByTagName("Year");
                if (nodes.getLength() > 0) {
                    String year = ((Element)nodes.item(0)).getTextContent();
                    try {
                        props.put("year", Integer.parseInt(year));
                    }
                    catch (NumberFormatException ex) {
                        Logger.warn("Bogus year: "+year);
                    }
                }
            }
            
            dn = kg.createNodeIfAbsent(props, URI_P);
            dn.addTag("KS:"+ksp.getId());
            for (Map.Entry<MeSH, String[]> me : meshes.entrySet()) {
                MeSH mesh = me.getKey();
                KNode xn = createMeshNode (kg, mesh);
                if (xn.getId() != dn.getId()) {
                    xn.addTag("KS:"+ksp.getId());
                    KEdge e = kg.createEdgeIfAbsent(dn, xn, mesh.name);
                    if (me.getValue().length > 0)
                        e.put("qualifier", me.getValue());
                    e.put("ui", mesh.ui);
                }
            }
        }
        
        return dn;
    }

    KNode createMeshNode (KGraph kg, MeSH mesh) throws Exception {
        Map<String, Object> p = mesh.encode(new TreeMap<>());
        KNode xn = kg.createNodeIfAbsent(p, URI_P);
        Logger.debug(" ++ MeSH node "+xn.getId()+" created for "+mesh.ui
                     +" "+mesh.name);
        for (String tree : mesh.treeNumbers) {
            Logger.debug("  ... linking "+tree);
            String[] paths = tree.split("\\.");
            if (paths.length > 0) {
                StringBuilder path = new StringBuilder (paths[0]);
                for (int i = 1; i < paths.length; ++i) {
                    KNode[] nodes =
                        kg.findNodes("treeNumbers", path.toString());
                    Logger.debug("    + "+path+" => "+nodes.length);
                    for (KNode n : nodes) {
                        if (n.getId() != xn.getId()) {
                            kg.createEdgeIfAbsent(xn, n, path.toString());
                        }
                    }
                    path.append("."+paths[i]);
                }
                
                KNode[] nodes = kg.findNodes("treeNumbers", path.toString());
                Logger.debug("    + "+path+" => "+nodes.length);
                for (KNode n : nodes) {
                    if (n.getId() != xn.getId()) {
                        kg.createEdgeIfAbsent(xn, n, path.toString());
                    }
                }
            }
        }
        
        return xn;
    }

    public void resolvePubmed (String pmid, KNode kn, KGraph kg)
        throws Exception {
        WSRequest req = wsclient.url(EUTILS_BASE+"/efetch.fcgi")
            .setFollowRedirects(true)
            .setQueryParameter("db", "pubmed")
            .setQueryParameter("rettype", "xml")
            .setQueryParameter("id", pmid)
            ;
        Logger.debug("+++ resolving..."+req.getUrl());
        
        WSResponse res = req.get().toCompletableFuture().get();
        Logger.debug("+++ parsing..."+res.getUri());
        
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri() + " returns status "
                        + res.getStatus());
            return;
        }
        
        Document doc = fromInputSource
            (new InputSource (new ByteArrayInputStream (res.asByteArray())));

        KNode dn = instrumentDoc (doc, kg);
        if (dn != null)
            kg.createEdgeIfAbsent(dn, kn, pmid);
    }

    boolean checkTreeNumber (String node) {
        // whitelist is used to override blacklist
        for (String t : whitelist)
            if (node.startsWith(t))
                return true;
        for (String t : blacklist)
            if (node.startsWith(t))
                return false;
        return true;
    }

    static String getLastToken (String s, String p) {
        int pos = s.lastIndexOf(p);
        if (pos > 0) {
            s = s.substring(pos+p.length());
        }
        return s;
    }

    String[] getTreeNumbers (String ui) throws Exception {
        Set<String> treeNums = new TreeSet<>();
        Logger.debug(" ++ checking tree number: "+ui);
        
        // https://hhs.github.io/meshrdf/sample-queries
        WSRequest req = wsclient.url("https://id.nlm.nih.gov/mesh/sparql")
            .setFollowRedirects(true)
            .setQueryParameter
            ("query",
             "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"+
             "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"+
             "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"+
             "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"+
             "PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>\n"+
             "PREFIX mesh: <http://id.nlm.nih.gov/mesh/>\n"+
             "PREFIX mesh2015: <http://id.nlm.nih.gov/mesh/2015/>\n"+
             "PREFIX mesh2016: <http://id.nlm.nih.gov/mesh/2016/>\n"+
             "PREFIX mesh2017: <http://id.nlm.nih.gov/mesh/2017/>\n"+
             "PREFIX mesh2018: <http://id.nlm.nih.gov/mesh/2018/>\n"+
             "SELECT *\n"+
             "FROM <http://id.nlm.nih.gov/mesh>\n"+
             "WHERE {\n"+
             "  mesh:"+ui+" meshv:treeNumber ?treeNum .\n"+
             "}\n"+
             "ORDER BY ?label")
            .setQueryParameter("format", "json")
            .setQueryParameter("limit", "50")
            ;
        
        WSResponse res = req.get().toCompletableFuture().get();
        Logger.debug("  ++++ "+req.getUrl()+"..."+res.getStatus());
        
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri() + " returns status "
                        + res.getStatus());
        }
        else {
            JsonNode json = res.asJson().get("results").get("bindings");
            for (int i = 0; i < json.size(); ++i) {
                String url = json.get(i).get("treeNum").get("value").asText();
                String treeNum = getLastToken (url, "/");
                Logger.debug(" => "+treeNum);
                if (checkTreeNumber (treeNum))
                    treeNums.add(treeNum);
            }
        }

        // filter for only the mesh in specific tree numbers here!
        return treeNums.toArray(new String[0]);
    }

    MeSH[] queryMeSH (String query) throws Exception {
        Map<String, MeSH> meshes = new TreeMap<>();
        WSRequest req = wsclient.url("https://id.nlm.nih.gov/mesh/sparql")
            .setFollowRedirects(true)
            .setQueryParameter
            ("query",
             "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n"+
             "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"+
             "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"+
             "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n"+
             "PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>\n"+
             "PREFIX mesh: <http://id.nlm.nih.gov/mesh/>\n"+
             "PREFIX mesh2015: <http://id.nlm.nih.gov/mesh/2015/>\n"+
             "PREFIX mesh2016: <http://id.nlm.nih.gov/mesh/2016/>\n"+
             "PREFIX mesh2017: <http://id.nlm.nih.gov/mesh/2017/>\n"+
             "PREFIX mesh2018: <http://id.nlm.nih.gov/mesh/2018/>\n"+
             " SELECT ?d ?dName ?treeNumber\n"+
             " FROM <http://id.nlm.nih.gov/mesh>\n"+
             " WHERE {\n"+
             " ?d a meshv:Descriptor .\n"+
             " ?d meshv:active 1 .\n"+
             " ?d rdfs:label ?dName .\n"+
             " ?d meshv:treeNumber ?treeNumber\n"+
             " FILTER(REGEX(?dName,\""+query+"\",\"i\"))\n"+
             " }\n"+
             " ORDER BY ?d ?treeNumber\n")
            .setQueryParameter("format", "json")
            .setQueryParameter("limit", "50")
            .setQueryParameter("inference", "true")
            ;

        Logger.debug(" ++ query mesh: "+query);

        WSResponse res = req.get().toCompletableFuture().get();
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri() + " returns status "
                        + res.getStatus());
        }
        else {
            JsonNode json = res.asJson().get("results").get("bindings");
            for (int i = 0; i < json.size(); ++i) {
                JsonNode n = json.get(i);
                String value = n.get("d").get("value").asText();
                String ui = getLastToken (value, "/");
                MeSH m = meshes.get(ui);
                if (m == null) {
                    meshes.put(ui, m = new MeSH (ui));
                    m.name = n.get("dName").get("value").asText();
                    Logger.debug(" => matched "+m.ui+" "+m.name);
                }
                value = n.get("treeNumber").get("value").asText();
                m.treeNumbers.add(getLastToken (value, "/"));
            }
            Logger.debug(" ++ matching MeSH terms: "+meshes.size());
        }

        return meshes.values().toArray(new MeSH[0]);
    }
    
    
    protected void seedDrug(JsonNode idList, KNode kn, KGraph kg)
    {
        Map<String, String> params = new HashMap<>();
        params.put("dbfrom","pubmed");
        params.put("db","pccompound");
        params.put("retmode","json");
        String url = ksp.getUri()+"/elink.fcgi";
        if(idList.isArray())
        {
            for(int i=0;i<idList.size();++i)
            {
                JsonNode currentId = idList.get(i);
                params.put("id",currentId.asText());
                resolve(url,params,kn,kg,this::resolveDrug);

            }
        }
    }
    protected void resolveDrug(JsonNode json, KNode kn, KGraph kg)
    {
        Optional<JsonNode> links = resolveELink(json, kn, kg, "pubmed_pccompound_mesh");
        if(links.isPresent())
        {
            seedPubChem(links.get(),kn,kg);
        }

    }
    //This is currently working under the assumption that while it is an array, LinkSets is only
    //ever of size one.
    protected Optional<JsonNode> resolveELink(JsonNode json, KNode kn, KGraph kg, String linkName) {
        JsonNode linkSets = json.get("linksets");
        if(linkSets.size()>1)
        {
            Logger.error("Unexpected result in resolveELink");
        }
        if (linkSets.isArray()) {
            for (int i = 0; i < linkSets.size(); ++i) {
                if (linkSets.get(i).has("linksetdbs")) {
                    JsonNode linkSetDbs = linkSets.get(i).get("linksetdbs");
                    if (linkSetDbs.isArray()) {
                        for (int j = 0; j < linkSetDbs.size(); ++j) {
                            JsonNode currentNode = linkSetDbs.get(i);
                            if (currentNode.get("linkname").asText().equals(linkName)) {
                                JsonNode links = currentNode.get("links");
                                return Optional.of(links);
                            }
                        }
                    } else {
                        JsonNode links = linkSetDbs.get("links");
                        return Optional.of(links);
                    }
                }
            }

        }
        return Optional.empty();
    }
    protected void seedPubChem(JsonNode json, KNode kn, KGraph kg)
    {
        Map<String,String> params = new HashMap<>();
        params.put("db","pccompound");
        params.put("retmode","json");
        String url = ksp.getUri()+"/esummary.fcgi";
        if(json.isArray())
        {
            for(int i = 0; i<json.size();i++)
            {
                params.put("id",json.get(i).asText());
                resolve(url,params,kn,kg,this::resolvePubChem);
            }
        }
    }
    protected void resolvePubChem(JsonNode json, KNode kn, KGraph kg)
    {
        JsonNode results = json.get("result");
        JsonNode uids = results.get("uids");
        Map<String,Object> props = new HashMap<>();
        if(uids.isArray())
        {
            for(int i = 0;i<uids.size();i++)
            {
                JsonNode result = results.get(uids.get(i).asText());
                System.out.println(result.get("synonymlist").get(0).asText());
                props.put(NAME_P,result.get("synonymlist").get(0).asText());
                props.put(TYPE_P,"drug");
                props.put(URI_P,"https://pubchem.ncbi.nlm.nih.gov/compound/"+uids.get(i).asText());
                KNode xn = kg.createNodeIfAbsent(props, URI_P);
                if (xn.getId() != kn.getId()) {
                    xn.addTag("KS:" + ksp.getId());
                    kg.createEdgeIfAbsent(kn, xn, "resolve");
                }
            }
        }

    }
    protected void seedGene(JsonNode idList, KNode kn, KGraph kg)
    {
        String url = ksp.getUri()+"/elink.fcgi";
        Map<String,String> params = new HashMap<>();
        params.put("dbfrom","pubmed");
        params.put("db","gene");
        params.put("retmode","json");
        if(idList.isArray())
        {
            for(int i=0;i<idList.size();++i)
            {
                JsonNode currentId = idList.get(i);
                params.put("id",currentId.asText());
                resolve(url,params,kn,kg,this::resolveGeneList);
            }
        }

    }
    protected void resolveGeneList(JsonNode json, KNode kn, KGraph kg)
    {
        Optional<JsonNode> links = resolveELink(json, kn, kg, "pubmed_gene");
        if(links.isPresent())
        {
            JsonNode linkNode = links.get();
            String url = "https://www.ncbi.nlm.nih.gov/gene/";
            Map<String,String> params = new HashMap<>();
            params.put("report","xml");
            params.put("format","text");
            System.out.println("LINKSIZE: "+linkNode.size());
            for(int i =0;i<linkNode.size();i++)
            {
                params.put("term",linkNode.get(i).asText());
                resolveXml(url,params,kn,kg,this::resolveGene);
            }

        }
    }
    protected void resolveGene(JsonNode json, KNode kn, KGraph kg)
    {
        String geneId =
                json.get("Entrezgene")
                .get("Entrezgene_track-info")
                .get("Gene-track")
                .get("Gene-track_geneid")
                .asText();
        String geneName = json.get("Entrezgene")
                .get("Entrezgene_gene")
                .get("Gene-ref")
                .get("Gene-ref_locus")
                .asText();
        Map<String,Object> props = new HashMap<>();
        props.put(NAME_P,geneName);
        props.put(TYPE_P,"gene");
        props.put(URI_P,"https://www.ncbi.nlm.nih.gov/gene/"+geneId);
        KNode xn = kg.createNodeIfAbsent(props, URI_P);
        if (xn.getId() != kn.getId()) {
            xn.addTag("KS:" + ksp.getId());
            kg.createEdgeIfAbsent(kn, xn, "resolve");
        }

    }
    
    void resolve (String url, Map<String, String> params,
                  KNode kn, KGraph kg, Resolver resolver) {
        WSRequest req = wsclient.url(url).setFollowRedirects(true);
        if (params != null) {
            for (Map.Entry<String, String> me : params.entrySet()) {
                Logger.debug(".."+me.getKey()+": "+me.getValue());
                req = req.setQueryParameter(me.getKey(), me.getValue());
                Logger.debug(me.getKey()+", "+me.getValue());
            }
        }
        Logger.debug("+++ resolving..."+req.getUrl());

        try {
            System.out.println(req.get().toCompletableFuture().get().getUri());
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            resolver.resolve(json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }

    //This method is a travesty
    void resolveXml (String url, Map<String, String> params,
                  KNode kn, KGraph kg, Resolver resolver) {
        WSRequest req = wsclient.url(url).setFollowRedirects(true);
        if (params != null) {
            for (Map.Entry<String, String> me : params.entrySet()) {
                Logger.debug(".."+me.getKey()+": "+me.getValue());
                req = req.setQueryParameter(me.getKey(), me.getValue());
                Logger.debug(me.getKey()+", "+me.getValue());
            }
        }
        Logger.debug("+++ resolving..."+req.getUrl());

        try {
            System.out.println(req.get().toCompletableFuture().get().getUri());
            WSResponse res = req.get().toCompletableFuture().get();
            ObjectMapper mapper = new ObjectMapper();
            String XMLString =res.getBody().replaceAll("<!DOCTYPE[^>]*>\n", "");
            XMLString=XMLString.replaceAll("&lt;","<")
                    .replaceAll("&gt;",">")
                    .replace("<pre>","")
                    .replace("</pre>","");

            JSONObject jsonObject = XML.toJSONObject(XMLString);
            JsonNode json = mapper.readValue(jsonObject.toString(), JsonNode.class);
            resolver.resolve(json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
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
//            Logger.warn(res.getUri()+" returns status "+res.getStatus());
            return;
        }

        JsonNode json = res.asJson().get("esearchresult");
        if (json != null) {
            int count = json.get("count").asInt();
            JsonNode list = json.get("idlist");
            if (count > 0) {
                for (int i = 0; i < count; ++i) {
                    if(list.get(i)!=null) {
                        long pmid = list.get(i).asLong();
                        processDoc(kgraph, pmid);
                    }
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
        NodeList nodes = doc.getElementsByTagName("MeshHeading");
        for(int i = 0; i < nodes.getLength(); i++)
        {
            //add if-has element
            Element element = (Element) nodes.item(i);
            NodeList meshHeadingList = element.getChildNodes();

            NodeList headings = element.getElementsByTagName("DescriptorName");
            for(int j = 0; j<headings.getLength(); j++)
            {
                Element heading = (Element)headings.item(j);
                Logger.debug("Topic: "+heading.getTextContent());
                String meshId = heading.getAttribute("UI");

                WSRequest meshReq = wsclient.url("https://id.nlm.nih.gov/mesh/"+meshId+".json-ld");
                WSResponse meshRes = meshReq.get().toCompletableFuture().get();
                TimeUnit.SECONDS.sleep(2);
                if (200 != meshRes.getStatus()) {
                    Logger.warn(meshRes.getUri()+" returns status "+meshRes.getStatus());
                    return;
                }
                try {
                    JsonNode node = meshRes.asJson();
                    JsonNode treeNode = node.findValue("treeNumber");
                    treeNode.forEach(tn->{
                        Logger.debug(tn.asText());
                    });
                } catch (Exception ex) {
                    ex.printStackTrace();
                }

            }
        }
        //TODO: parse this document into knodes..
        /*
        Care about:
            Diseases
            Chemicals and Drugs
                Node of drug, edge of action -> node of target
            Analytical, Diagnostic and Therapeutic Techniques, and Equipment
            Phenomena and Processes
         */
        if (nodes.getLength() == 0) {
            Logger.warn(pmid+": Not a valid PubMed DOM!");
            return;
        }

        Element article = (Element)nodes.item(0);
        if (nodes.getLength() > 0) {
            Logger.debug(pmid+": "+nodes.item(0).getTextContent());
        }
    }
    void resolveArticles (JsonNode json, KNode kn, KGraph kg) {
        Map<String, Object> props = new TreeMap<>();
        props.put(TYPE_P, "pubmed");
    }
}
