package blackboard.semmed;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.regex.*;
import java.util.*;
import java.sql.*;
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

import akka.actor.ActorSystem;

import blackboard.*;
import blackboard.umls.UMLSKSource;
import static blackboard.KEntity.*;


public class SemMedDbKSource implements KSource {
    public final WSClient wsclient;
    public final KSourceProvider ksp;
    public final UMLSKSource umls;
    
    private final Database db;
    private final CacheApi cache;
    private final Map<String, Set<String>> blacklist;
    private final Map<String, Set<String>> whitelist;
    private final Integer minPredCount;

    static class Evidence {
        Double score;
        final String pmid;
        final String context;
        Evidence (String pmid, String context) {
            this.pmid = pmid;
            this.context = context;
        }
    }
    
    static class Triple {
        public final String subject;
        public final String subtype;
        public final String predicate;
        public final String object;
        public final String objtype;
        public List<Evidence> evidence = new ArrayList<>();
        
        Triple (String subject, String subtype, String predicate,
                String object, String objtype) {
            this.subject = subject;
            this.subtype = subtype;
            this.predicate = predicate;
            this.object = object;
            this.objtype = objtype;
        }
    }
    
    @Inject
    public SemMedDbKSource (WSClient wsclient, CacheApi cache,
                            @Named("semmed") KSourceProvider ksp,
                            @NamedDatabase("semmed") Database db,
                            UMLSKSource umls,
                            ApplicationLifecycle lifecycle) {
        this.wsclient = wsclient;
        this.ksp = ksp;
        this.cache = cache;
        this.db = db;
        this.umls = umls;

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
                return F.Promise.pure(null);
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
        try (Connection con = db.getConnection();
             PreparedStatement pstm1 = con.prepareStatement
             ("select subject_cui,subject_semtype,predicate,"
              +"object_cui,object_semtype,count(*) as cnt "
              +"from PREDICATION where SUBJECT_CUI = ? "
              +"group by subject_cui,predicate,object_cui "
              +"having cnt > ? order by cnt desc");
             PreparedStatement pstm2 = con.prepareStatement
             ("select subject_cui,subject_semtype,predicate,"
              +"object_cui,object_semtype,count(*) as cnt "
              +"from PREDICATION where OBJECT_CUI = ? "
              +"group by subject_cui,predicate,object_cui "
              +"having cnt > ? order by cnt desc");
             ) {
            Logger.debug("++ resolving "+cui+"...");
            long start = System.currentTimeMillis();
            List<Triple> triples = new ArrayList<>();
            resolvePredicates (triples, pstm1, cui);
            resolvePredicates (triples, pstm2, cui);
            Logger.debug("..."+triples.size()+" predicates in "
                         +String.format("%1$.3fs!",
                                        (System.currentTimeMillis()-start)
                                        /1000.));
        }
    }

    protected void resolvePredicates (List<Triple> triples,
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
                String subject = rset.getString("subject_cui");
                String subtype = rset.getString("subject_semtype");
                String pred = rset.getString("predicate");
                String object = rset.getString("object_cui");
                String objtype = rset.getString("object_semtype");

                int pos = subject.indexOf('|');
                if (pos > 0)
                    subject = subject.substring(0, pos);
                pos = object.indexOf('|');
                if (pos > 0)
                    object = object.substring(0, pos);

                if (isBlacklist ("semtype", subtype)
                    || isBlacklist ("semtype", objtype)
                    || isBlacklist ("subject_cui", subject)
                    || isBlacklist ("object_cui", object)) {
                }
                else {
                    int count = rset.getInt("cnt");
                    Triple t = new Triple (subject, subtype,
                                           pred, object, objtype);
                    Logger.debug(subject+" ["+subtype+"] =="
                                 +pred+"=> "+object+" ["+objtype+"] "
                                 +count);
                    pstm2.setString(1, subject);
                    pstm2.setString(2, pred);
                    pstm2.setString(3, object);
                    ResultSet rs = pstm2.executeQuery();
                    while (rs.next()) {
                        String pmid = rs.getString("pmid");
                        String sent = rs.getString("sentence");
                        t.evidence.add(new Evidence (pmid, sent));
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
        try (Connection con = db.getConnection();
             PreparedStatement pstm = con.prepareStatement
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
}
