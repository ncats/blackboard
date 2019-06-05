package blackboard.pubmed.index;

import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import com.typesafe.config.Config;
import play.api.Configuration;
import play.inject.ApplicationLifecycle;
import play.cache.SyncCacheApi;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;

import blackboard.umls.UMLSKSource;
import static blackboard.index.Index.*;
import static blackboard.pubmed.index.PubMedIndex.*;
import blackboard.Util;

import com.fasterxml.jackson.databind.JsonNode;
import gov.nih.nlm.nls.metamap.Result;

@Singleton
public class PubMedIndexManager implements AutoCloseable {

    static class MetaMapActor extends AbstractActor {
        static Props props (UMLSKSource umls) {
            return Props.create
                (MetaMapActor.class, () -> new MetaMapActor (umls));
        }
        
        final UMLSKSource umls;
        public MetaMapActor (UMLSKSource umls) {
            this.umls = umls;
        }
        
        @Override
        public void preStart () {
            Logger.debug("### "+self ()+ "...initialized!");
        }

        @Override
        public void postStop () {
            Logger.debug("### "+self ()+"...stopped!");
        }

        @Override
        public Receive createReceive () {
            return receiveBuilder()
                .match(TextQuery.class, this::doMetaMap)
                .build();
        }

        void doMetaMap (TextQuery q) throws Exception {
            Logger.debug(self()+": metamap "+q);
            try {
                long start = System.currentTimeMillis();
                JsonNode result = umls.getMetaMap().annotateAsJson(q.query);
                Logger.debug(self()+": metamap executed in "+String.format
                             ("%1$.3fs",
                              1e-3*(System.currentTimeMillis()-start)));

                List<Concept> concepts =
                    PubMedIndex.parseMetaMapConcepts(result);
                getSender().tell(concepts, getSelf ());
            }
            catch (Exception ex) {
                Logger.error("Can't execute MetaMap", ex);
            }
        }
    }
    
    static class PubMedIndexActor extends AbstractActor {
        static Props props (PubMedIndexFactory pmif, File db) {
            return Props.create
                (PubMedIndexActor.class, () -> new PubMedIndexActor (pmif, db));
        }
        
        final PubMedIndex pmi;
        public PubMedIndexActor (PubMedIndexFactory pmif, File dir) {
            pmi = pmif.get(dir);
        }

        @Override
        public void preStart () {
            Logger.debug("### "+self ()+ "...initialized!");
        }

        @Override
        public void postStop () {
            try {
                pmi.close();
            }
            catch (Exception ex) {
                Logger.error("Can't close PubMedIndex: "+pmi.getDbFile(), ex);
            }
            Logger.debug("### "+self ()+"...stopped!");
        }

        @Override
        public Receive createReceive () {
            return receiveBuilder()
                .match(TextQuery.class, this::doTextSearch)
                .match(PMIDQuery.class, this::doPMIDSearch)
                .build();
        }

        void doTextSearch (TextQuery q) throws Exception {
            Logger.debug(self()+": searching "+q);

            long start = System.currentTimeMillis();
            SearchResult result = pmi.search(q, q.skip+q.top); 
            Logger.debug(self()+": search completed in "+String.format
                         ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start)));
            
            getSender().tell(result, getSelf ());
        }

        void doPMIDSearch (PMIDQuery q) throws Exception {
            Logger.debug(self()+": fetching "+q.pmid);
            long start = System.currentTimeMillis();
            MatchedDoc doc = pmi.getMatchedDoc(q.pmid);
            Logger.debug(self()+": fetch completed in "+String.format
                         ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start)));
            getSender().tell(doc, getSelf ());
        }
    }
    
    final List<ActorRef> indexes = new ArrayList<>();
    final ActorRef metamap;
    final ActorSystem actorSystem;
    final int maxTimeout, maxTries, maxHits;
    final SyncCacheApi cache;
    final UMLSKSource umls;
    
    @Inject
    public PubMedIndexManager (Configuration config, PubMedIndexFactory pmif,
                               UMLSKSource umls, ActorSystem actorSystem,
                               SyncCacheApi cache,
                               ApplicationLifecycle lifecycle) {
        Config conf = config.underlying().getConfig("app.pubmed");
        
        if (!conf.hasPath("indexes"))
            throw new IllegalArgumentException
                ("No app.pubmed.indexes property defined!");

        File dir = new File
            (conf.hasPath("base") ? conf.getString("base") : ".");
        if (!dir.exists())
            throw new IllegalArgumentException
                ("base path "+dir+" doesn't exist!");

        maxTimeout = conf.hasPath("max-timeout")
            ? conf.getInt("max-timeout") : 10;
        maxTries = conf.hasPath("max-tries") ? conf.getInt("max-tries") : 5;
        maxHits = conf.hasPath("max-hits") ? conf.getInt("max-hits") : 20;
        
        List<String> indexes = conf.getStringList("indexes");
        for (String idx : indexes) {
            File db = new File (dir, idx);
            try {
                ActorRef actorRef = actorSystem.actorOf
                    (PubMedIndexActor.props(pmif, db), idx);
                this.indexes.add(actorRef);
            }
            catch (Exception ex) {
                Logger.error("Can't load database: "+db, ex);
            }
        }

        metamap = actorSystem.actorOf(MetaMapActor.props(umls));
        lifecycle.addStopHook(() -> {
                close ();
                return CompletableFuture.completedFuture(null);
            });

        this.umls = umls;
        this.actorSystem = actorSystem;
        this.cache = cache;
        
        Logger.debug("$$$$ "+getClass().getName()
                     +": base="+dir
                     +" max-hits="+maxHits
                     +" max-timeout="+maxTimeout
                     +" indexes="+indexes);
    }

    public void close () throws Exception {
        metamap.tell(PoisonPill.getInstance(), ActorRef.noSender());
        for (ActorRef actorRef : indexes) {
            actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        Logger.debug("$$ shutting down "+getClass().getName());
    }

    public SearchResult search (String query, Map<String, Object> facets) {
        return search (query, facets, 0, maxHits);
    }

    public SearchResult search (Map<String, Object> facets, int skip, int top) {
        return search (null, facets, skip, top);
    }

    public SearchResult search (String query, Map<String, Object> facets,
                                int skip, int top) {
        return search (null, query, facets, skip, top);
    }
    
    public SearchResult search (String field, String query,
                                Map<String, Object> facets,
                                int skip, int top) {
        final TextQuery tq = new TextQuery (field, query, facets);
        tq.skip = skip;
        tq.top = top;
        
        return cache.getOrElseUpdate
            (tq.cacheKey()+"/"+skip+"/"+top, new Callable<SearchResult>() {
                    public SearchResult call () {
                        return search (tq);
                    }
                });
    }

    public SearchResult facets () {
        final TextQuery tq = new TextQuery ();
        return cache.getOrElseUpdate
            (PubMedIndexManager.class.getName()+"/facets",
             new Callable<SearchResult>() {
                    public SearchResult call () {
                        return search (tq);
                    }
                });        
    }

    protected SearchResult search (TextQuery tq) {
        Logger.debug("#### Query: "+tq);
        Inbox inbox = Inbox.create(actorSystem);

        if (tq.query != null) {
            // first pass this through metamap
            inbox.send(metamap, tq);
            try {
                List<Concept> concepts = (List<Concept>)
                    inbox.receive(Duration.ofSeconds(5));
                Logger.debug("#### "+concepts.size()
                             +" concept(s) found from query "+tq);
                tq.concepts.addAll(concepts);
            }
            catch (TimeoutException ex) {
                Logger.warn("Unable to process query with MetMap: "+tq);
            }
        }
        
        // now do the search
        for (ActorRef actorRef : indexes)
            inbox.send(actorRef, tq);
        
        List<SearchResult> results = new ArrayList<>();
        for (int i = 0, ntries = 0; i < indexes.size()
                 && ntries < maxTries;) {
            try {
                SearchResult result = (SearchResult)inbox.receive
                    (Duration.ofSeconds(maxTimeout));
                results.add(result);
                ++i;
            }
            catch (TimeoutException ex) {
                ++ntries;
                Logger.warn("Unable to receive result from Inbox"
                            +" within alloted time; retrying "+ntries);
            }
        }
        
        return PubMedIndex.merge(tq.skip, tq.top,
                                 results.toArray(new SearchResult[0]));
    }
    

    public MatchedDoc getDoc (Long pmid, String format) {
        final PMIDQuery q = new PMIDQuery (pmid);
        return cache.getOrElseUpdate
            (q.cacheKey()+"/"+format, new Callable<MatchedDoc>() {
                    public MatchedDoc call () {
                        Inbox inbox = Inbox.create(actorSystem);
                        List<MatchedDoc> docs = new ArrayList<>();
                        for (ActorRef actorRef : indexes) {
                            inbox.send(actorRef, q);
                            try {
                                MatchedDoc doc = (MatchedDoc)inbox.receive
                                    (Duration.ofSeconds(2l));
                                if (doc != EMPTY_DOC)
                                    docs.add(doc);
                            }
                            catch (TimeoutException ex) {
                                Logger.error("Unable to receive result from "
                                             +actorRef
                                             +" within alloted time", ex);
                            }
                        }
                        
                        if (docs.size() > 1)
                            Logger.warn(pmid+" has multiple ("
                                        +docs.size()+") documents!");
                        return docs.isEmpty() ? null : docs.get(0);
                    }
                });
    }
}
