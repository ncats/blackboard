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
//import com.sun.xml.internal.bind.v2.TODO;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeCreator;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
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
import play.libs.ws.ahc.AhcWSClient;
import play.mvc.BodyParser;

import static blackboard.KEntity.*;

public class PubMedKSource implements KSource {
    private final WSClient wsclient;
    private final KSourceProvider ksp;
    private final CacheApi cache;
    
    private final String EUTILS_BASE;
    private final String MESH_BASE;
    
    private final HashMap<Character,String>meshMap;

    private static final Set<String> MOLECULE = new HashSet<>(Arrays.asList(
            "D23","D25","D09","D27","D20","D03","D06","D01","D10","D05","D13","D02","D26","D04"));
    private static final Set<String> TARGET = new HashSet<>(Arrays.asList(
            "D12","D27"));
    private static final Set<String> CELLTYPE = new HashSet<>(Arrays.asList(
            "A11"));
    private static final Set<String> DISEASES = new HashSet<>(Arrays.asList(
            "C"));
    private static final Set<String> PATHWAY = new HashSet<>(Arrays.asList(
            "G"));
    private static final Set<String> ANATOMY = new HashSet<>(Arrays.asList(
            "A"));
    private static final Set<String> BANLIST = new HashSet<>(Arrays.asList(
            "A11.251.210"/*cell lines*/,"I03"/*exercise*/,"G07.690.773.875", "G07.690.936.500",/*Drug Dose Relationship*/
            "G02.111.570", "G02.466"/*Molecular Structure*/, "G07.690.936.563" /*Area Under Curve*/,"A18","A19","A13"/*Plants and animals*/));


    interface Resolver {
        void resolve (JsonNode json, KNode kn, KGraph kg);
    }
    interface XMLReadables{
    }
    static Map<String, Set<String>> MESHCATS;
    static ActorSystem system = ActorSystem.create("WSClient");

    static WSClient createWSClient () {
        ActorMaterializer materializer;
        AsyncHttpClientConfig config;

        config = new DefaultAsyncHttpClientConfig.Builder()
                .setMaxRequestRetry(0)
                .setShutdownQuietPeriod(0)
                .setShutdownTimeout(0).build();

        ActorMaterializerSettings settings = ActorMaterializerSettings.create(system);
        materializer = ActorMaterializer.create(settings, system, "WSClient");
        return new AhcWSClient(config, materializer);
    }

    public static void main(String[] args) throws Exception
    {
//        getReferencedPapers(4423606);
        File inputFile = new File("/Users/williamsmard/Desktop/desc2017.xml");
        InputStream is = new FileInputStream(inputFile);
        DocumentBuilder db1 = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document doc1 = db1.parse(is);
        MESHCATS = loadMeshCategories (doc1);
        is.close();
        describeStructure();
        System.exit(0);
        String input = "/Users/williamsmard/Documents/COPTest.csv";
        BufferedReader br = Files.newBufferedReader(Paths.get(input));
        List<String> lines = br.lines().collect(Collectors.toList());
        for(int z = 0; z<lines.size();z++)
        {
            String term1 = lines.get(z).split(",")[0];
            String term2 = lines.get(z).split(",")[1];
            //String term1 = args[0];
            //String term2 = args[1];
            DateTime time = new DateTime();

            List<Set<String>>route = new ArrayList<Set<String>>(Arrays.asList(TARGET,PATHWAY,ANATOMY));
            List<LinkedList<String>> paths = new ArrayList<LinkedList<String>>();
            LinkedList<String> init = new LinkedList<String>();
            init.add(term1);
            paths.add(init);
//
//            File inputFile = new File("/Users/williamsmard/Desktop/desc2017.xml");
//            InputStream is = new FileInputStream(inputFile);
//            DocumentBuilder db1 = DocumentBuilderFactory.newInstance().newDocumentBuilder();
//            Document doc1 = db1.parse(is);
//            MESHCATS = loadMeshCategories (doc1);
//            is.close();


            HashSet<String> topics = new HashSet();
            for(int i=0;i<route.size();i++)
            {
                System.out.println(i);
                System.out.println("PathsSize: "+paths.size());
                List<LinkedList<String>> tempPaths= new ArrayList<LinkedList<String>>();
                for(int j=0;j<paths.size();j++)
                {
                    paths.get(j).iterator().forEachRemaining(node->{
                        System.out.print(node+"->");
                    });
                    System.out.println();
                    List<Long> papers = getPapers(paths.get(j).getLast(),5);
                    for(int k =0;k<papers.size();k++)
                    {
                        try{
                            topics.addAll(getTopics(papers.get(k),route.get(i)));
                        }
                        catch(Exception ex)
                        {
                            ex.printStackTrace();
                            System.err.println("Error getting topics for paper "+papers.get(k));
                            continue;
                        }
                    }
                    Iterator<String> striter = topics.iterator();
                    while(striter.hasNext())
                    {
                        LinkedList temp = new LinkedList(paths.get(j));
                        temp.add(striter.next());
                        tempPaths.add(temp);
                    }
                }
                paths=tempPaths;
            }
            List<Long> finalPapers = getPapers(term2,25);
            HashSet<String>finalTopics = new HashSet();
            for(int i = 0;i<finalPapers.size();i++)
            {
                try
                {
                    finalTopics.addAll(getTopics(finalPapers.get(i),route.get(route.size()-1)));
                }
                catch(Exception ex)
                {
                    System.err.println("Unable to form end-point topics list");
                    ex.printStackTrace();
                    System.exit(1);
                }
            }
            Iterator<LinkedList<String>>pathsIterator = paths.iterator();
            List<LinkedList<String>>removalList=new ArrayList<LinkedList<String>>();
            while(pathsIterator.hasNext())
            {
                LinkedList currentList = pathsIterator.next();
                if(!finalTopics.contains(currentList.getLast())){
                    removalList.add(currentList);
                }
            }

            paths.removeAll(removalList);
            File outputFile = new File("/Users/williamsmard/Documents/COPLogs/"+term1+"_"+term2+time.getHourOfDay()+time.getMinuteOfHour());
            try{
                PrintWriter writer = new PrintWriter(outputFile);
                paths.forEach(path->{
                    path.iterator().forEachRemaining(node->{
                        writer.print(node+"->");
                    });
                    writer.println(term2);
                });
            } catch (IOException e) {
                // do something
            }

        }


        System.exit(0);
    }

    //Method to get a list of PMIDs for pubmed papers on the topic 'term'
    public static List getPapers(String term, int limit)
    {
        WSClient client = createWSClient();
        WSRequest req = client.url("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi")
                .setFollowRedirects(true)
                .setQueryParameter("db", "pubmed")
                .setQueryParameter("retmax", "100")
                .setQueryParameter("retmode", "json")
                .setQueryParameter("term", "\"Pharmacologic Actions\"[MeSH Major Topic] AND \""+term+"\"");
        List papers = new ArrayList();

        try {
            WSResponse res = req.get().toCompletableFuture().get();
            client.close();

            if (200 != res.getStatus()) {
                //Logger.warn(res.getUri()+" returns status "+res.getStatus());
                return papers;
            }
            JsonNode json = res.asJson().get("esearchresult");
            if (json != null) {
                int count = json.get("count").asInt();
                JsonNode list = json.get("idlist");
                if (count > 0) {
                    for (int i = 0; i < count; ++i) {
                        if(list.get(i)!=null) {
                            long pmid = list.get(i).asLong();
                            papers.add(pmid);
                            if(papers.size()>limit)
                            {
                                return papers;
                            }
                        }
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return papers;
    }
    public static HashSet<String> getReferencedPapers(long pmid)
    {
        HashSet<String> Ids = new HashSet<String>();
        //https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=pmc&linkname=pmc_refs_pubmed&id=4423606
        try {
            WSClient client = createWSClient();
            WSRequest req = client.url("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi")
                    .setFollowRedirects(true)
                    .setQueryParameter("dbfrom", "pmc")
                    .setQueryParameter("linkname", "pmc_refs_pubmed")
                    .setQueryParameter("retmode", "json")
                    .setQueryParameter("id", String.valueOf(pmid));
            WSResponse res = req.get().toCompletableFuture().get();
            client.close();

            if (200 != res.getStatus()) {
                Logger.warn(res.getUri()+" returns status "+res.getStatus());
                return null;
            }
            JsonNode json = res.asJson();
            List<JsonNode> idNodes = json.findValues("links");
            Iterator<JsonNode> nodeIterator = idNodes.iterator();
            while(nodeIterator.hasNext())
            {
                JsonNode currentId = nodeIterator.next();
                Arrays.asList(currentId.toString().split(",")).forEach(id->{
                    Ids.add(id.replaceAll("\\D",""));
                });
            }

        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            return null;
        }
        //Ids.forEach(id->{System.out.println(id);});
        return Ids;
    }
    //Method to get a HashSet of MeshTopics in a pubmed paper specified by 'pmid'
    public static HashSet<String> getTopics(long pmid, Set<String> acceptableCategories) throws Exception
    {
        HashSet<String> topics = new HashSet<String>();


        try{
            WSClient client = createWSClient ();
            WSRequest req = client.url("https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi")
                    .setFollowRedirects(true)
                    .setQueryParameter("db", "pubmed")
                    .setQueryParameter("retmode", "xml")
                    .setQueryParameter("id", String.valueOf(pmid));


            Logger.debug("fetching "+pmid);
            WSResponse res = req.get().toCompletableFuture().get();
            client.close();

            if (200 != res.getStatus()) {
                //Logger.warn(res.getUri()+" returns status "+res.getStatus());
                return topics;
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
                    String topic = heading.getTextContent();
                    String meshId = heading.getAttribute("UI");
                    Set<String> categories = MESHCATS.get(meshId);//egetMeshCategory(meshId,client);
                    //TimeUnit.MILLISECONDS.sleep(150);
                    if (categories == null) {
                    //no-op
                   }
                    else if(!categories.isEmpty() && acceptableCategories!=null){
                        //Take only categories in our list of acceptable ones
                        if(categories.stream().filter(cat->{
                            for(String acceptable : acceptableCategories){
                                if (cat.startsWith(acceptable)) {
                                    return true;}}return false;}).findAny().isPresent()){
                            topics.add(topic);
                        }
                        //Even from those that are acceptable, throw out those that are on the global banlist
                        if(categories.stream().filter(cat->{
                            for(String banned : BANLIST){
                                if(cat.startsWith(banned)){
                                    return true;}}return false;}).findAny().isPresent()){
                                topics.remove(topic);
                                }

                    }
                    else if(acceptableCategories==null)
                    {
                        topics.add(topic);
                    }
                }
            }


        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            return topics;
        }
        return topics;
    }

    public static Map<String, Set<String>> loadMeshCategories (Document doc) throws Exception {
        Map<String, Set<String>> descriptors = new HashMap<>();
        NodeList nodes = doc.getElementsByTagName("DescriptorUI");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Node n = nodes.item(i);
            String descriptor = n.getTextContent();
            NodeList nl = ((Element)n.getParentNode()).getElementsByTagName("TreeNumber");
            Set<String> treenumbers = new TreeSet<>();
            for (int j = 0; j < nl.getLength(); ++j)
                treenumbers.add(((Element)nl.item(j)).getTextContent());

            if (!treenumbers.isEmpty()) {
                descriptors.put(descriptor, treenumbers);
//                System.out.println(descriptor + ": " + treenumbers);
            }
        }
        return descriptors;
    }

    public static Set<String> getMeshCategory(String meshId,Document doc) throws Exception
    {
        Set<String> categories = new HashSet<String>();
//        File inputFile = new File("/Users/williamsmard/Desktop/desc2017.xml");
        XPathFactory factory = XPathFactory.newInstance();
        XPath xPath = factory.newXPath();
        StringBuilder sb = new StringBuilder();
        sb.append("//DescriptorRecord/DescriptorUI[.=\"");
        sb.append(meshId);
        sb.append("\"]/..//TreeNumber");
        XPathExpression expr = xPath.compile(sb.toString());
        NodeList n = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);
//            System.out.println("length: "+n.getLength());
       System.out.println(sb.toString());
        for(int i = 0; i<n.getLength();i++)
        {
            Node node = n.item(i);
            categories.add(node.getTextContent());
        }


        return categories;
    }
    public static Set<String> getMeshCategory(String meshId,WSClient client) throws Exception
    {
        Set<String> categories = new HashSet<String>();
        System.out.println(meshId);
        WSRequest meshReq = client.url("https://id.nlm.nih.gov/mesh/"+meshId+".json-ld");
        WSResponse meshRes = meshReq.get().toCompletableFuture().get();
        if (200 != meshRes.getStatus()) {
            //Logger.warn(meshRes.getUri()+" returns status "+meshRes.getStatus());
            System.out.println(meshRes.getStatus());
            return categories;
        }
        try {
            JsonNode node = meshRes.asJson();
            JsonNode treeNode = node.findValue("treeNumber");
            if(treeNode!=null)
            {
                //TODO Fix magic number that is currently trimming out URL portion of treeNumber
                treeNode.forEach(tn -> {
                    categories.add(tn.asText().substring(27));
                });
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return categories;
    }
    
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

        if (EUTILS_BASE == null)
            throw new IllegalArgumentException
                (ksp.getId()+" doesn't have \"uri\" property defined!");
        if (MESH_BASE == null)
            throw new IllegalArgumentException
                (ksp.getId()+" doesn't have \"mesh\" property defined!");

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        this.meshMap=createMeshMap();
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());
    }
    
    public HashMap<Character,String> createMeshMap() {
        HashMap<Character,String> meshMap = new HashMap<Character, String>();
        meshMap.put('C',"Diseases");
        meshMap.put('D',"Chemicals and Drugs");
        meshMap.put('G',"Phenomena and Processes");
        meshMap.put('E',"Analytical, Diagnostic and Therapeutic Techniques, and Equipment");
        return meshMap;
    }
    
    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        try {
            for (KNode kn : nodes) {
                seedGeneric(kn,kgraph);
//                switch (kn.getType()) {
//                case "query":
//                    //seedQuery (kgraph, n);
//                    //No-oping this for now, until reasonable performance can be achieved
//                    seedGeneric(kn,kgraph);
//                    break;
//
//                case "disease":
//                    break;
//                case "drug":
//                    break;
//                case "protein":
//                    break;
//                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't execute knowledge source on kgraph "
                         +kgraph.getId(), ex);
        }
    }

    protected void seedGeneric (KNode kn, KGraph kg) {
        String term;
        if(kn.get("term")!=null) {
            term = kn.get("term").toString();
        }
        else {
            term = kn.get("name").toString();
        }
        String url = ksp.getUri() + "/esearch.fcgi";
        try {
            Map<String, String> q = new HashMap<>();
            q.put("db", "pubmed");
            q.put("retmax", "50");
            q.put("retmode", "json");
            q.put("sort","relevance");
            q.put("term", term);
            resolve(ksp.getUri() + "/esearch.fcgi",
                    q, kn, kg, this::resolveGeneric);
        } catch (Exception ex) {
            Logger.error("Can't resolve url: " + url, ex);
        }
    }
    
    protected void resolveGeneric (JsonNode json, KNode kn, KGraph kg) {
        JsonNode esearchresult = json.get("esearchresult");
        JsonNode idList = esearchresult.get("idlist");
        //seedDrug (idList,kn,kg);
        //seedGene (idList,kn,kg);
        resolveMesh (idList,kn,kg);
    }
    
    protected void resolveMesh(JsonNode idList, KNode kn, KGraph kg) {
        Map<String, Object> props = new TreeMap<>();
        // FIXME: put this in config like uri & mesh
        for(int i = 0; i < 10; ++i) {
            resolveMesh (idList.get(i).asText(), kn, kg);
        }
    }

    public static Document fromInputSource (InputSource source)
        throws Exception {
        DocumentBuilderFactory factory =
            DocumentBuilderFactory.newInstance();
        factory.setFeature
            ("http://apache.org/xml/features/disallow-doctype-decl", false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        
        return builder.parse(source);
    }
    
    void resolveMesh (String pmid, KNode kn, KGraph kg) {
        WSRequest req = wsclient.url(EUTILS_BASE+"/efetch.fcgi")
            .setFollowRedirects(true)
            .setQueryParameter("db", "pubmed")
            .setQueryParameter("rettype", "xml")
            .setQueryParameter("id", pmid)
            ;
        Logger.debug("+++ resolving..."+req.getUrl());
        
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            Logger.debug("+++ parsing..."+res.getUri());
            
            if (200 != res.getStatus()) {
                Logger.warn(res.getUri() + " returns status "
                            + res.getStatus());
                return;
            }
                
            Document doc = fromInputSource
                (new InputSource (new ByteArrayInputStream
                                  (res.asByteArray())));
            
            NodeList nodes = doc.getElementsByTagName("MeshHeading");
            if (nodes.getLength() > 0) {
                for(int j = 0; j < nodes.getLength(); j++) {
                    //add if-has element
                    Element element = (Element) nodes.item(j);
                    NodeList headings =
                        element.getElementsByTagName("DescriptorName");
                    for(int k = 0; k<headings.getLength(); k++) {
                        Element heading = (Element) headings.item(k);
                        final String meshId = heading.getAttribute("UI");
                        Set<String> treeNums = cache.getOrElse
                            (meshId, new Callable<Set<String>> () {
                                    public Set<String> call ()
                                        throws Exception {
                                        return getTreeNumbers (meshId);
                                    }
                                });
                        
                        String topic = heading.getTextContent();
                        Map<String, Object> props = new TreeMap<>();
                        props.put(NAME_P, topic);
                        props.put(TYPE_P, "MeSH");
                        props.put(XREF_P,
                                  "https://ncbi.nlm.nih.gov/pubmed/"+pmid);
                        props.put(URI_P, MESH_BASE+"/"+meshId);
                        KNode xn = kg.createNodeIfAbsent(props, URI_P);
                        if (xn.getId() != kn.getId()) {
                            xn.addTag("KS:"+ksp.getId());
                            kg.createEdgeIfAbsent(kn, xn, meshId);
                        }
                    }
                }
            }
        }
        catch(Exception ex) {
            ex.printStackTrace();
            Logger.error("Can't resolve MeSH for "+pmid, ex);
        }
    }

    Set<String> getTreeNumbers (String ui) throws Exception {
        return getTreeNumbers (ui, null);
    }
    
    Set<String> getTreeNumbers (String ui, Set<String> treeNums)
        throws Exception {
        if (treeNums == null)
            treeNums = new TreeSet<>();
        
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
             "SELECT *\n"+
             "FROM <http://id.nlm.nih.gov/mesh>\n"+
             "WHERE {\n"+
             "  mesh:"+ui+" meshv:treeNumber ?treeNum .\n"+
             "}\n"+
             "ORDER BY ?label")
            .setQueryParameter("format", "json")
            .setQueryParameter("limit", "50")
            ;
        Logger.debug(" ++ checking tree number: "+ui);

        WSResponse res = req.get().toCompletableFuture().get();
        if (200 != res.getStatus()) {
            Logger.warn(res.getUri() + " returns status "
                        + res.getStatus());
        }
        else {
            JsonNode json = res.asJson().get("results").get("bindings");
            for (int i = 0; i < json.size(); ++i) {
                String url = json.get(i).get("treeNum").get("value").asText();
                int pos = url.lastIndexOf('/');
                if (pos > 0) {
                    String treeNum = url.substring(pos+1);
                    Logger.debug(" => "+treeNum);
                    treeNums.add(treeNum);
                }
                else {
                    Logger.warn("Bogus treeNum: "+url);
                }
            }
        }
        
        return treeNums;
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
    private static Map<String,String> getStructureMap()
    {
        Map<String,String>structureMap = new HashMap<>();
        Statement statement = null;
        ResultSet results = null;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:mysql://chembl.ncats.io/chembl_23","chembl_23","chembl_23");
            statement = connection.createStatement();
            results = statement.executeQuery("SELECT docs.pubmed_id,compound_structures.standard_inchi_key FROM docs LEFT JOIN activities ON " +
                    "docs.doc_id = activities.doc_id LEFT JOIN compound_structures ON activities.molregno=compound_structures.molregno WHERE docs.pubmed_id IS NOT NULL LIMIT 10");
            while(results.next())
            {

                structureMap.put(results.getString(1),results.getString(2));
            }
        }
        catch (SQLException ex){
            ex.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally
        {
            if(statement != null)
            {
                try{
                    statement.close();
                }
                catch(SQLException sqlEx)
                {
                    sqlEx.printStackTrace();
                }
                statement=null;
            }

            if(results!=null)
            {
                try{
                    results.close();
                }
                catch(SQLException sqlEx)
                {
                    sqlEx.printStackTrace();
                }
                results=null;
            }
        }
        return  structureMap;
    }
    private static void describeStructure()
    {
        Map struct2pub=getStructureMap();
        Map<String,Set<String>> descriptors = new HashMap();
        Set<String> empty = new HashSet<>();
        struct2pub.forEach((pub,chembl)->{
            try {
                System.out.println(chembl.toString());
                System.out.println(getTopics(Long.parseLong(pub.toString()),null).toString());
                //descriptors.put(chembl.toString(),getTopics(Long.parseLong(pub.toString()),null));

            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        descriptors.forEach((chembl,catSet)->{
            System.out.println(chembl);
            catSet.forEach(cat->{
                System.out.println(cat);
            });
        });
    }
    public String prettyPrint (JsonNode json)
    {
        StringBuilder sb = new StringBuilder();
        Iterator<String> i = json.fieldNames();
        while(i.hasNext())
        {
            String next = i.next();
            sb.append(next);
            sb.append(": ");
            sb.append(json.get(next));
            sb.append("\n");
        }
        return sb.toString();
    }

}
