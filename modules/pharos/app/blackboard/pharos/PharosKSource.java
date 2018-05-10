package blackboard.pharos;

import java.util.*;
import java.util.function.BiConsumer;
import java.net.URLEncoder;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Named;

import play.Logger;
import play.Configuration;
import play.libs.ws.*;
import play.inject.ApplicationLifecycle;
import play.libs.F;
import akka.actor.ActorSystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import blackboard.*;
import blackboard.pubmed.PubMedKSource;
import static blackboard.KEntity.*;

public class PharosKSource implements KSource {
    interface Resolver {
        void resolve (JsonNode json, KNode kn, KGraph kg);
    }
    
    private final ActorSystem actorSystem;
    private final WSClient wsclient;
    private final KSourceProvider ksp;
    private final PubMedKSource pubmedKS;
    
    
    @Inject
    public PharosKSource (ActorSystem actorSystem, WSClient wsclient,
                          @Named("pharos") KSourceProvider ksp,
                          PubMedKSource pubmedKS,
                          ApplicationLifecycle lifecycle) {
        this.actorSystem = actorSystem;
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.pubmedKS = pubmedKS;

        lifecycle.addStopHook(() -> {
                wsclient.close();
                return F.Promise.pure(null);
            });
        
        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp);
    }

    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        
        if (nodes == null || nodes.length == 0)
            nodes = kgraph.getNodes();

        for (KNode kn : nodes) {
            switch (kn.getType()) {
            case "query":
                seedQuery ((String)kn.get("term"), kn, kgraph);
                break;
                
            case "disease":
                seedDisease (kn, kgraph);
                break;
                
            case "protein":
                seedTarget (kn, kgraph);
                break;
                
            case "drug":
                seedLigand (kn, kgraph);
                break;
            default:
                if(kn.get("name")!=null)
                {
                    seedQuery(kn.get("name").toString(),kn,kgraph);
                }
                break;
            }
        }
    }
    
    void seedQuery (String term, KNode kn, KGraph kg) {
        Logger.debug(">>> seedQuery \""+term+"\"");
        try {
            Map<String, String> q = new HashMap<>();
            q.put("q", "\""+term+"\"");
            q.put("facet","IDG Development Level/Tclin");
            q.put("top", "20");
            
            resolve (ksp.getUri()+"/targets/search",
                     q, kn, kg, this::resolveTargets);
            
            resolve (ksp.getUri()+"/ligands/search",
                     q, kn, kg, this::resolveLigands);
            
            resolve (ksp.getUri()+"/diseases/search",
                     q, kn, kg, this::resolveDiseases);
        }
        catch (Exception ex) {
            Logger.error("Unable to utf encode query \""+term+"\"", ex);
        }
    }

    void seedTarget (KNode kn, KGraph kg) {
        Logger.debug(">>> seedTarget \""+kn.getName()+"\"");
        String uri = (String) kn.get(URI_P);
        if (uri != null && uri.startsWith(ksp.getUri())) {
            Map<String, String> params = new HashMap<>();
            params.put("top", "20");
            // argh.. should update the pharos api to allow list for filter
            resolve (uri+"/links(kind=ix.idg.models.Ligand)", params,
                     kn, kg, this::resolveLinks);
            resolve (uri+"/links(kind=ix.idg.models.Disease)", params,
                     kn, kg, this::resolveLinks);
            // extract the id from the uri
            int pos = uri.lastIndexOf("/targets(");
            if (pos > 0) {
                String s = uri.substring(pos+9);
                s = s.substring(0, s.indexOf(')'));
                try {
                    long id = Long.parseLong(s);
                    resolveTargetPPI (id, kn, kg);
                }
                catch (NumberFormatException ex) {
                    Logger.debug("Bogus target id: "+s);
                }
            }
            else {
                Logger.debug("Not a recognized uri: "+uri);
            }
        }
        else if (kn.getName() != null) {
            Map<String, String> query = new HashMap<>();
            query.put("filter", "name='"+kn.getName()+"'");
            query.put("top", "20");
            resolve (ksp.getUri()+"/targets", query, 
                     kn, kg, this::resolveTargets);
        }
    }

    void seedLigand (KNode kn, KGraph kg) {
        Logger.debug(">>> seedLigand \""+kn.getName()+"\"");
        String uri = (String) kn.get(URI_P);
        if (uri != null && uri.startsWith(ksp.getUri())) {
            Map<String, String> params = new HashMap<>();
            params.put("top", "20");
            resolve (uri+"/links(kind=ix.idg.models.Target)", params,
                     kn, kg, this::resolveLinks);
            resolve (uri+"/links(kind=ix.idg.models.Disease)", params,
                     kn, kg, this::resolveLinks);
        }
        else if (kn.getName() != null) {
            Map<String, String> query = new HashMap<>();
            query.put("filter", "name='"+kn.getName()+"'");
            query.put("top", "20");
            resolve (ksp.getUri()+"/ligands", query,
                     kn, kg, this::resolveLigands);
        }
    }

    void seedDisease (KNode kn, KGraph kg) {
        Logger.debug(">>> seedDisease \""+kn.getName()+"\"");
        String uri = (String) kn.get(URI_P);
        if (uri != null && uri.startsWith(ksp.getUri())) {
            Map<String, String> params = new HashMap<>();
            params.put("top", "10"); // return 20 max
            resolve (uri+"/links(kind=ix.idg.models.Target)", params,
                     kn, kg, this::resolveLinks);
            resolve (uri+"/links(kind=ix.idg.models.Ligand)", params,
                     kn, kg, this::resolveLinks);
            params.put("top","999999");
            resolve (uri+"/links(kind=ix.idg.models.Target)", params,
                    kn, kg, this::resolveLinks);
        }
        else if (kn.getName() != null) {
            Map<String, String> query = new HashMap<>();
            query.put("filter", "name='"+kn.getName()+"'");
            query.put("top","20");
            resolve (ksp.getUri()+"/diseases", query,
                     kn, kg, this::resolveDiseases);
        }
    }

    void resolve (String url, Map<String, String> params,
                  KNode kn, KGraph kg, Resolver resolver) {
        WSRequest req = wsclient.url(url).setFollowRedirects(true);
        if (params != null) {
            Logger.debug(url);
            for (Map.Entry<String, String> me : params.entrySet()) {
                //Logger.debug(".."+me.getKey()+": "+me.getValue());
                req = req.setQueryParameter(me.getKey(), me.getValue());
            }
        }
        Logger.debug("+++ resolving..."+req.getUrl());
        
        try {   
            WSResponse res = req.get().toCompletableFuture().get();
            Logger.debug("+++ url: "+res.getUri());
            JsonNode json = res.asJson();
            resolver.resolve(json, kn, kg);
        }
        catch (Exception ex) {
            Logger.error("Can't resolve url: "+url, ex);
        }
    }
    
    void instrument (String entity, JsonNode json, KNode kn, KGraph kg,
                     BiConsumer<JsonNode, Map<String, Object>> consumer) {
        String uri = null;
        if (json.hasNonNull("uri"))
            uri = json.get("uri").asText();
        
        JsonNode content = json.get("content"); 
        for (int i = 0; i < content.size(); ++i) {
            JsonNode jn = content.get(i);
            long id = jn.get("id").asLong();
            String name = jn.get("name").asText();
            Map<String, Object> props = new TreeMap<>();
            props.put(URI_P, ksp.getUri()+"/"+entity+"("+id+")");
            props.put(NAME_P, name);
            consumer.accept(jn, props);
            
            KNode node = kg.createNodeIfAbsent(props, URI_P);
            if (node.getId() != kn.getId()) {
                node.addTag("KS:"+ksp.getId());
                props.clear();
                props.put("value", uri);
                kg.createEdgeIfAbsent(kn, node, "resolve", props, null);
                
                if ("targets".equals(entity))
                    resolveTargetGeneRIF (id, node, kg);
                Logger.debug(node.getId()+"..."+name);
            }
        }
        Logger.debug("uri: "+json.get(URI_P).asText()+"..."+content.size());    
    }

    void resolveTargets (JsonNode json, KNode kn, KGraph kg) {
        instrument ("targets", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "protein");       
                props.put("family", jn.get("idgFamily").asText());
                props.put("tdl", jn.get("idgTDL").asText());
                if (jn.hasNonNull("accession"))
                    props.put("uniprot", jn.get("accession").asText());
                if (jn.hasNonNull("gene"))
                    props.put("gene", jn.get("gene").asText());
                if (jn.hasNonNull("description"))
                    props.put("description", jn.get("description").asText());
                String[] syns = retrieveSynonyms
                    ((String)props.get(URI_P), "(label=UniProt*)");
                if (syns.length > 0)
                    props.put(SYNONYMS_P, syns);
            });
    }

    void resolveTargetPPI (long id, KNode kn, KGraph kg) {
        // grab protein-protein interaction
        WSRequest req = wsclient.url(ksp.getUri()+"/predicates")
            .setQueryParameter
            ("filter",
             "predicate='Protein-Protein Interactions' AND subject.refid="+id);
        Logger.debug("Resolving PPI for target "+id+"...");
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode content = res.asJson().get("content");
            for (int i = 0; i < content.size(); ++i) {
                JsonNode ppi = content.get(i);
                if (ppi.hasNonNull("objects")) {
                    JsonNode objs = ppi.get("objects");
                    Logger.debug("PPI: target="+id+" "+objs.size());
                    for (int j = 0; j < Math.min(10, objs.size()); ++j)
                        resolveTargetLink (objs.get(j), "ppi", kn, kg);
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't resolve PPI for "+id, ex);
        }
    }

    void resolveTargetGeneRIF (long id, KNode kn, KGraph kg) {
        WSRequest req = wsclient.url
            (ksp.getUri()+"/targets/"+id+"/links(kind=ix.core.models.Text)");
        Logger.debug("Resolving geneRIF for target "+id+"...");
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode content = res.asJson();
            // TODO: parameterize this!
            for (int i = 0; i < Math.min(20, content.size()); ++i) {
                JsonNode n = content.get(i).get("properties");
                for (int j = 0; j < n.size(); ++j) {
                    JsonNode p = n.get(j);
                    if ("PubMed ID".equals(p.get("label").asText())) {
                        String pmid = p.get("intval").asText();
                        pubmedKS.resolvePubmed(pmid, kn, kg);
                    }
                }
            }
        }
        catch (Exception ex) {
            Logger.error("Can't resolve geneRIF for "+id, ex);
        }
    }

    void resolveLigands (JsonNode json, KNode kn, KGraph kg) {
        instrument ("ligands", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "drug");
                String[] syns = retrieveSynonyms
                    ((String)props.get(URI_P), null);
                if (syns.length > 0)
                    props.put(SYNONYMS_P, syns);
            });
    }

    void resolveDiseases (JsonNode json, KNode kn, KGraph kg) {
        instrument ("diseases", json, kn, kg, (jn, props) -> {
                props.put(TYPE_P, "disease");
                String[] syns = retrieveSynonyms
                    ((String)props.get(URI_P), null);
                if (syns.length > 0)
                    props.put(SYNONYMS_P, syns);                
            });
    }

    void resolveLinks (JsonNode json, KNode kn, KGraph kg) {
        resolveLinks (json, null, kn, kg);
    }
    
    void resolveLinks (JsonNode json, String type, KNode kn, KGraph kg) {
        if (json.isArray()) {
            for (int i = 0; i < json.size(); ++i) {
                resolveLinks (json.get(i), type, kn, kg); // recurse.. 
            }
        }
        else if (json.hasNonNull("kind")) {
            String kind = json.get("kind").asText();
            switch (kind) {
            case "ix.idg.models.Ligand":
                resolveLigandLink (json, type, kn, kg);
                break;
                
            case "ix.idg.models.Target":
                resolveTargetLink (json, type, kn, kg);
                break;
                
            case "ix.idg.models.Disease":
                resolveDiseaseLink (json, type, kn, kg);
                break;
            }
        }
    }

    void resolveLigandLink (JsonNode node, String type, KNode kn, KGraph kg) {
        long id = node.get("refid").asLong();
        JsonNode pn = node.get("properties");
        
        String name = null, href = null;
        for (int j = 0; j < pn.size(); ++j) {
            JsonNode n = pn.get(j);
            // we only care if this ligand has a known pharmalogical action
            if ("Pharmalogical Action".equals(n.get("label").asText())) {
                type = n.get("term").asText(); // ignore whatever type is
                if (n.hasNonNull("href"))
                    href = n.get("href").asText();
            }
            else if ("IDG Ligand".equals(n.get("label").asText())) {
                name = n.get("term").asText();
            }
        }

        // disease -> ligand can have empty properties, so we just
        // make
        if (type != null || pn.size() == 0) {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "drug");
            String uri = ksp.getUri()+"/ligands("+id+")";
            props.put(URI_P, uri);
            if (name != null)
                props.put(NAME_P, name);
            
            // create this node if it isn't on the graph already
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                if (name == null)
                    xn.putIfAbsent(NAME_P, () -> {
                            return retrieveJsonValue (uri+"/$name");
                        });
                xn.putIfAbsent(SYNONYMS_P, () -> {
                        return retrieveSynonyms (uri, null);
                    });
                
                // now link it
                if (href != null) {
                    props.clear();
                    props.put("href", href);
                }
                kg.createEdgeIfAbsent(xn, kn, type != null
                                      ? type.toLowerCase() : "assertion",
                                      props, null);
                Logger.debug(xn.getId()+":"+xn.getName()
                             + " <-> "+kn.getId()+":"+kn.getName());
            }
        }
    }

    void resolveDiseaseLink (JsonNode node, String type, KNode kn, KGraph kg) {
        long id = node.get("refid").asLong();
        JsonNode pn = node.get("properties");

        String ds = null, disease = null;
        for (int i = 0; i < pn.size(); ++i) {
            JsonNode n = pn.get(i);
            switch (n.get("label").asText()) {
            case "Data Source":
                {
                    String term = n.get("term").asText();
                    // only do these data source for now
                    /*if ("DisGeNET".equals(term))
                        ds = term;
                        else*/
                    if ("DrugCentral Indication".equals(term))
                        ds = "indication";
                }
                break;
                
            case "IDG Disease":
                disease = n.get("term").asText();
                break;
            }
        }

        if (type != null || ds != null) {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "disease");
            String uri = ksp.getUri()+"/diseases("+id+")";
            props.put(URI_P, uri);
            props.put(NAME_P, disease);

            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                xn.putIfAbsent(SYNONYMS_P, () -> {
                        return retrieveSynonyms (uri, null);                    
                    });
                
                KEdge ke = kg.createEdgeIfAbsent
                    (kn, xn, type != null ? type : ds);
                Logger.debug(kn.getId()+":"+kn.getName()
                             + " <-> "+xn.getId()+":"+xn.getName());
            }
        }
    }

    void resolveTargetLink (JsonNode node, KNode kn, KGraph kg) {
        resolveTargetLink (node, null, kn, kg);
    }

    void resolveTargetLink (JsonNode node, String type, KNode kn, KGraph kg) {
        long id = node.get("refid").asLong();
        JsonNode pn = node.get("properties");

        String name = null, ds = null, tdl = null; 
        for (int i = 0; i < pn.size(); ++i) {
            JsonNode n = pn.get(i);
            switch (n.get("label").asText()) {
            case "Pharmalogical Action":
                if (n.hasNonNull("term"))
                    type = n.get("term").asText();
                break;
                
            // unfortunately this isn't a target name.. sigh
            case "IDG Target":
                name = n.get("term").asText();
                break;

            case "Data Source":
                ds = n.get("term").asText();
                break;

            case "IDG Development Level":
                if ("Tclin".equals(n.get("term").asText()))
                    tdl = "Tclin";
                break;
            }
        }
        //Logger.debug("link "+node.get("id").asText()+": type="+type+" tdl="+tdl);
        if (type != null || tdl != null) {
            Map<String, Object> props = new TreeMap<>();
            props.put(TYPE_P, "protein");
            String uri = ksp.getUri()+"/targets("+id+")";
            props.put(URI_P, uri);
            
            KNode xn = kg.createNodeIfAbsent(props, URI_P);
            if (xn.getId() != kn.getId()) {
                xn.addTag("KS:"+ksp.getId());
                /*
                xn.putIfAbsent(NAME_P, () -> {
                        return retrieveJsonValue (uri+"/$name");
                    });
                */
                resolve (uri, null, xn, kg, (json, n, g) -> {
                        n.put(NAME_P, json.get("name").asText());
                        n.put("family", json.get("idgFamily").asText());
                        n.put("tdl", json.get("idgTDL").asText());
                        if (json.hasNonNull("description"))
                            n.put("description",
                                  json.get("description").asText());
                    });
                xn.putIfAbsent(SYNONYMS_P, () -> {
                        return retrieveSynonyms (uri, null);
                    });
                
                
                KEdge ke = kg.createEdgeIfAbsent
                    (kn, xn, type != null ? type.toLowerCase() : "assertion");
                Logger.debug(kn.getId()+":"+kn.getName()
                             + " <-> "+xn.getId()+":"+xn.getName());
            }
        }
    }

    String[] retrieveSynonyms (String url, String filter) {
        List<String> syns = new ArrayList<>();
        // only retrieve UniProt related synonyms
        WSRequest req = wsclient.url(url+"/synonyms"
                                     +(filter != null ? filter:""));
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode json = res.asJson();
            for (int i = 0; i < json.size(); ++i) {
                String s = json.get(i).get("term").asText();
                if (syns.indexOf(s) < 0) // terrible
                    syns.add(s);
            }
        }
        catch (Exception ex) {
            Logger.error("Can't get synonyms for "+url, ex);
        }
        //Logger.debug(url+" => "+syns.size()+" synonyms!");
        
        return syns.toArray(new String[0]);
    }

    String retrieveJsonValue (String url) {
        WSRequest req = wsclient.url(url);
        try {
            WSResponse res = req.get().toCompletableFuture().get();
            return res.getBody();
        }
        catch (Exception ex) {
            Logger.error("Can't get Json value for "+url, ex);
        }
        return null;
    }
    HashSet<String> retrieveDiseaseIds(String term)
    {
        HashSet<String> diseaseIds = new HashSet<>();
        WSRequest req = wsclient.url(ksp.getUri()+"/diseases/search");
            req.setQueryParameter("q",term);
        try
        {
            WSResponse res = req.get().toCompletableFuture().get();
            JsonNode diseaseNode = res.asJson();
            JsonNode diseaseContent = diseaseNode.get("content");
            if(diseaseContent.isArray())
            {
                for(int i=0;i<diseaseContent.size();i++)
                {
                    JsonNode currentContent = diseaseContent.get(i);
                    diseaseIds.add(currentContent.get("id").asText());
                }
            }
        }
        catch(Exception ex)
        {
            Logger.error(ex.getStackTrace().toString());
        }
        return diseaseIds;
    }
    HashSet<String>retrieveDiseaseTargets(HashSet<String> diseaseIds)
    {
        HashSet<String> diseaseTargets = new HashSet<>();
        diseaseIds.forEach(disease->{
            WSRequest req = wsclient.url(ksp.getUri()+"/diseases("+disease+")/links");
            try
            {
                WSResponse res = req.get().toCompletableFuture().get();
                JsonNode links = res.asJson();
                for(int i = 0;i<links.size();i++)
                {
                    JsonNode currentLink = links.get(i);
                    if(currentLink.get("kind").asText().equals("ix.idg.models.Target"))
                    {
                        diseaseTargets.add(currentLink.get("refid").asText());
                    }
                }

            }
            catch(Exception ex)
            {
                Logger.error(ex.getStackTrace().toString());
            }
        });
        return diseaseTargets;
    }
}
