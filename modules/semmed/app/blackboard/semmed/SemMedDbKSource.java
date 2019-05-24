package blackboard.semmed;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.inject.Named;
import java.util.regex.*;
import java.util.*;
import java.sql.*;
import java.io.*;
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
import play.mvc.BodyParser;
import play.mvc.Http;
import play.Environment;

import akka.actor.ActorSystem;

import blackboard.*;
import blackboard.umls.UMLSKSource;
import blackboard.umls.MatchedConcept;
import blackboard.pubmed.PubMedKSource;

import static blackboard.KEntity.*;

@Singleton
public class SemMedDbKSource implements KSource {
    public final WSClient wsclient;
    public final KSourceProvider ksp;
    public final UMLSKSource umls;
    public final PubMedKSource pubmed;
    
    private final Database db;
    private final SyncCacheApi cache;
    private final Map<String, Set<String>> blacklist;
    private final Map<String, Set<String>> whitelist;
    private final Integer minPredCount;
    
    @Inject
    public SemMedDbKSource (WSClient wsclient, SyncCacheApi cache,
                            Environment env,
                            @Named("semmed") KSourceProvider ksp,
                            @NamedDatabase("semmed") Database db,
                            UMLSKSource umls, PubMedKSource pubmed,
                            ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;
        this.db = db;
        this.umls = umls;
        this.pubmed = pubmed;

        blacklist = new ConcurrentHashMap<>();
        whitelist = new ConcurrentHashMap<>();
        // load extra data from semmed.json file (if exists)
        if (ksp.getData() != null) {
            loadList ("semtype");
            loadList ("subject_cui");
            loadList ("object_cui");
            loadList ("predicate");
        }

        String count = ksp.getProperties().get("min-predicate-count");
        minPredCount = count != null ? Integer.parseInt(count) : 10;


        lifecycle.addStopHook(() -> {
                wsclient.close();
                db.shutdown();
                return CompletableFuture.completedFuture(null);
            });

        Logger.debug("$"+ksp.getId()+": "+ksp.getName()
                     +" initialized; provider is "+ksp.getImplClass());        
    }

    public boolean isBlacklist (String kind, String value) {
        Set<String> set = blacklist.get(kind);
        if (set != null)
            return set.contains(value);
        return false;
    }

    public boolean isWhitelist (String kind, String value) {
        Set<String> set = whitelist.get(kind);
        if (set != null)
            return set.contains(value);
        return false;
    }

    void loadList (String kind) {
        JsonNode json = ksp.getData().get(kind);
        if (json != null) {
            JsonNode n = json.get("blacklist");
            if (n != null) {
                Set<String> set = new HashSet<>();
                blacklist.put(kind, set);
                for (int i = 0; i < n.size(); ++i)
                    set.add(n.get(i).asText());
                Logger.debug(kind+": "+set.size()+" blacklist entries!");
            }
            
            n = json.get("whitelist");
            if (n != null) {
                Set<String> set = new HashSet<>();
                whitelist.put(kind, set);
                for (int i = 0; i < n.size(); ++i)
                    set.add(n.get(i).asText());
                Logger.debug(kind+": "+set.size()+" whitelist entries!");
            }
        }
    }
    
    public void execute (KGraph kgraph, KNode... nodes) {
        Logger.debug("$"+ksp.getId()
                     +": executing on KGraph "+kgraph.getId()
                     +" \""+kgraph.getName()+"\"");
        
        for (KNode kn : nodes) {
            switch (kn.getType()) {
            case "concept": // cui
                { String cui = (String)kn.get("cui");
                    if (cui == null) {
                        Logger.warn(kn.getId()+": concept node doesn't "
                                    +"have a CUI!");
                    }
                    else {
                        try {
                            resolveCUI (cui, kn, kgraph);
                        }
                        catch (Exception ex) {
                            Logger.error("Can't resolve CUI "+cui, ex);
                        }
                    }
                }
                break;

            case "article":
                { String pmid = (String)kn.get("pmid");
                    if (pmid != null) {
                        try {
                            resolvePubmed (pmid, kn, kgraph);
                        }
                        catch (Exception ex) {
                            Logger.error("Can't resolve pubmed "+pmid, ex);
                        }
                    }
                    else {
                        Logger.warn(kn.getId()+": article has no pmid!");
                    }
                }
                break;
                
            default:
                Logger.debug(ksp.getId()+": can't resolve node of type \""
                             +kn.getType()+"\"");
            }
        }
    }

    public void resolveCUI (String cui, KNode kn, KGraph kg)
        throws Exception {
    }

    protected void instrument (List<Predication> triples,
                               PreparedStatement pstm, String cui)
        throws Exception {
        try (PreparedStatement pstm2 = pstm.getConnection()
             .prepareStatement("select * from PREDICATION a, SENTENCE b "
                               +"where subject_cui = ? and predicate = ? "
                               +"and object_cui = ? "
                               +"and a.sentence_id = b.sentence_id "
                               +"order by a.pmid")) {
            pstm.setString(1, cui);
            pstm.setInt(2, minPredCount);
            ResultSet rset = pstm.executeQuery();
            while (rset.next()) {
                String subcui = rset.getString("subject_cui");
                String subject = rset.getString("subject_name");
                String subtype = rset.getString("subject_semtype");
                String pred = rset.getString("predicate");
                String objcui = rset.getString("object_cui");
                String object = rset.getString("object_name");
                String objtype = rset.getString("object_semtype");

                int pos = subject.indexOf('|');
                if (pos > 0)
                    subcui = subcui.substring(0, pos);
                pos = objcui.indexOf('|');
                if (pos > 0)
                    objcui = objcui.substring(0, pos);

                if (isBlacklist ("semtype", subtype)
                    || isBlacklist ("semtype", objtype)
                    || isBlacklist ("subject_cui", subcui)
                    || isBlacklist ("object_cui", objcui)) {
                }
                else {
                    int count = rset.getInt("cnt");
                    Predication t = new Predication
                        (subcui, subject, subtype, pred,
                         objcui, object, objtype);
                    Logger.debug(subcui+" ["+subtype+"] =="
                                 +pred+"=> "+objcui+" ["+objtype+"] "
                                 +count);
                    pstm2.setString(1, subcui);
                    pstm2.setString(2, pred);
                    pstm2.setString(3, objcui);
                    ResultSet rs = pstm2.executeQuery();
                    while (rs.next()) {
                        Long id = rs.getLong("a.sentence_id");
                        String pmid = rs.getString("pmid");
                        String sent = rs.getString("sentence");
                        t.evidence.add(new Evidence (id, pmid, sent));
                    }
                    rs.close();
                    triples.add(t);
                }
            }
            rset.close();
        }
    }

    public void resolvePubmed (String pmid, KNode kn, KGraph kg)
        throws Exception {
        try (Connection conn = db.getConnection();
             PreparedStatement pstm = conn.prepareStatement
             ("select a.*,b.sentence from PREDICATION a, SENTENCE b where "
              +"a.PMID = ? AND a.SENTENCE_ID = b.SENTENCE_ID");
             ) {
            Logger.debug("++ resolve PMID "+pmid+"...");
            pstm.setString(1, pmid);
            ResultSet rset = pstm.executeQuery();
            int rows = 0;
            while (rset.next()) {
                String subtype = rset.getString("SUBJECT_SEMTYPE");
                String objtype = rset.getString("OBJECT_SEMTYPE");
                String pred = rset.getString("PREDICATE");
                String sub = rset.getString("SUBJECT_CUI");
                int pos = sub.indexOf('|');
                if (pos > 0)
                    sub = sub.substring(0, pos);
                String obj = rset.getString("OBJECT_CUI");
                pos = obj.indexOf('|');
                if (pos > 0)
                    obj = obj.substring(0, pos);
                
                if (isBlacklist ("semtype", subtype)
                    || isBlacklist ("semtype", objtype)
                    || isBlacklist ("subject_cui", sub)
                    || isBlacklist ("object_cui", obj)) {
                }
                else {
                    KNode sn = umls.createConceptNodeIfAbsent(kg, sub);
                    KNode on = umls.createConceptNodeIfAbsent(kg, obj);
                    if (sn != null && on != null) {
                        String sent = rset.getString("SENTENCE");
                        Map<String, Object> props = new HashMap<>();
                        props.put("article", pmid);
                        props.put("context", sent);
                        kg.createEdge(sn, on, pred, props);
                        kg.createEdgeIfAbsent(kn, sn, sub);
                        kg.createEdgeIfAbsent(kn, on, obj);
                        ++rows;
                    }
                }
            }
            rset.close();
            Logger.debug(rows+" nodes resolved!");
        }
    }

    public List<Predication> getPredications (final String cui)
        throws Exception {
        return cache.getOrElseUpdate
            ("semmed/"+cui, new Callable<List<Predication>> () {
                    public List<Predication> call () throws Exception {
                        return _getPredications (cui);
                    }
                });
                
    }
    
    public List<Predication> _getPredications (String cui) throws Exception {
        List<Predication> preds = new ArrayList<>();
        Logger.debug("++ resolving "+cui+"...");
        long start = System.currentTimeMillis();        
        try (Connection conn = db.getConnection();
             PreparedStatement pstm1 = conn.prepareStatement
             ("select subject_name,subject_cui,subject_semtype,predicate,"
              +"object_name,object_cui,object_semtype,count(*) as cnt "
              +"from PREDICATION where SUBJECT_CUI = ? "
              +"group by subject_cui,predicate,object_cui "
              +"having cnt > ? order by cnt desc");
             PreparedStatement pstm2 = conn.prepareStatement
             ("select subject_name,subject_cui,subject_semtype,predicate,"
              +"object_name,object_cui,object_semtype,count(*) as cnt "
              +"from PREDICATION where OBJECT_CUI = ? "
              +"group by subject_cui,predicate,object_cui "
              +"having cnt > ? order by cnt desc");
             ) {
            instrument (preds, pstm1, cui);
            instrument (preds, pstm2, cui);
        }
        Logger.debug("..."+preds.size()+" predicates in "
                     +String.format("%1$.3fs!",
                                    (System.currentTimeMillis()-start)
                                    /1000.));
        return preds;
    }

    public List<Predication> _getPredicationsByPMID (String pmid)
        throws Exception {
        List<Predication> preds = new ArrayList<>();
        long start = System.currentTimeMillis();
        try (Connection conn = db.getConnection();
             PreparedStatement pstm = conn.prepareStatement
             ("select a.*,b.sentence from PREDICATION a, SENTENCE b "
              +"where a.pmid = ? and a.sentence_id = b.sentence_id "
              +"order by b.number")) {
            pstm.setString(1, pmid);
            ResultSet rset = pstm.executeQuery();
            Map<Long, Predication> predmap = new TreeMap<>();
            Map<Long, Evidence> evmap = new TreeMap<>();
            while (rset.next()) {
                long predId = rset.getLong("PREDICATION_ID");
                Predication p = predmap.get(predId);
                if (p == null) {
                    String subject = rset.getString("SUBJECT_NAME");
                    String subtype = rset.getString("SUBJECT_SEMTYPE");
                    String objtype = rset.getString("OBJECT_SEMTYPE");
                    String pred = rset.getString("PREDICATE");
                    String subcui = rset.getString("SUBJECT_CUI");
                    int pos = subcui.indexOf('|');
                    if (pos > 0)
                        subcui = subcui.substring(0, pos);
                    String object = rset.getString("OBJECT_NAME");
                    String objcui = rset.getString("OBJECT_CUI");
                    pos = objcui.indexOf('|');
                    if (pos > 0)
                        objcui = objcui.substring(0, pos);
                    
                    if (isBlacklist ("semtype", subtype)
                        || isBlacklist ("semtype", objtype)
                        || isBlacklist ("subject_cui", subcui)
                        || isBlacklist ("object_cui", objcui)) {
                    }
                    else {
                        p = new Predication (subcui, subject, subtype, pred,
                                             objcui, object, objtype);
                        predmap.put(predId, p);
                    }
                }
                
                if (p != null) {
                    long sentId = rset.getLong("SENTENCE_ID");
                    Evidence ev = evmap.get(sentId);
                    if (ev == null) {
                        ev = new Evidence (rset.getLong("sentence_id"),
                                           rset.getString("pmid"),
                                           rset.getString("sentence"));
                        evmap.put(sentId, ev);
                    }
                    p.evidence.add(ev);
                }
            }
            rset.close();
            
            preds.addAll(predmap.values());            
            Logger.debug("++ fetch predications for PMID "
                         +pmid+"..."+preds.size());
        }
        return preds;
    }

    public List<Predication> getPredicationsByPMID (final String pmid)
        throws Exception {
        return cache.getOrElseUpdate
            ("semmed/"+pmid+"/pmid", new Callable<List<Predication>> () {
                    public List<Predication> call () throws Exception {
                        return _getPredicationsByPMID (pmid);
                    }
                });
    }
    

    public PredicateSummary getPredicateSummary (final String cui)
        throws Exception {
        return cache.getOrElseUpdate
            ("semmed/"+cui+"/summary", new Callable<PredicateSummary> () {
                    public PredicateSummary call () throws Exception {
                        return new PredicateSummary
                            (cui, getPredications (cui));
                    }
                });
    }

    /*
      PREDICATION_ID
      SENTENCE_ID
      PMID
      PREDICATE
      SUBJECT_CUI
      SUBJECT_NAME    
      SUBJECT_SEMTYPE
      SUBJECT_NOVELTY
      OBJECT_CUI
      OBJECT_NAME
      OBJECT_SEMTYPE
      OBJECT_NOVELTY
    */
    
}
