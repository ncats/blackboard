package blackboard.index.pubmed;

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
import play.libs.ws.*;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;

import blackboard.umls.UMLSKSource;
import static blackboard.index.Index.TextQuery;
import static blackboard.index.pubmed.PubMedIndex.*;
import blackboard.utils.Util;

import com.fasterxml.jackson.databind.JsonNode;

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
                .match(TextQuery.class, this::doSearch)
                .match(PMIDQuery.class, this::doPMIDSearch)
                .match(PMIDBatchQuery.class, this::doSearch)
                .match(FacetQuery.class, this::doFacetSearch)
                .build();
        }

        void doSearch (SearchQuery q) throws Exception {
            Logger.debug(self()+": searching "+q);
            long start = System.currentTimeMillis();
            SearchResult result = pmi.search(q);
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

        void doFacetSearch (FacetQuery fq) throws Exception {
            Logger.debug(self()+": facets "+fq);
            long start = System.currentTimeMillis();
            SearchResult result = pmi.facets(fq);
            Logger.debug(self()+": facets search completed in "+String.format
                         ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start)));
            getSender().tell(result, getSelf ());
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

    public SearchResult search (String query,
                                Map<String, Object> facets, int skip, int top) {
        return search (null, query, facets, skip, top, 1);
    }
    
    public SearchResult search (String field, String query,
                                Map<String, Object> facets,
                                int skip, int top, int slop) {
        final TextQuery tq = new TextQuery (field, query, facets);
        tq.skip = skip;
        tq.top = top;
        tq.slop = slop;
        
        return cache.getOrElseUpdate
            (tq.cacheKey()+"/"+skip+"/"+top, new Callable<SearchResult>() {
                    public SearchResult call () {
                        return search (tq);
                    }
                });
    }

    public SearchResult facets () {
        return facets (100);
    }

    public SearchResult facets (String facet, String value) {
        return facets (new FacetQuery (facet, value));
    }
    
    public SearchResult facets (int fdim) {
        return facets (new FacetQuery (fdim));
    }

    public SearchResult facets (Map<String, Object> facets) {
        return facets (new FacetQuery (facets));
    }

    public SearchResult facets (SearchQuery q) {
        Logger.debug("#### Facets: "+q);
        final String key = q.cacheKey();
        return cache.getOrElseUpdate(key, new Callable<SearchResult>() {
                public SearchResult call () {
                    Logger.debug("Cache missed: "+key);
                    return PubMedIndex.merge(getResults (q));
                }
            });
    }
    
    public List<Concept> _getConcepts (SearchQuery q) {
        Inbox inbox = Inbox.create(actorSystem);
        // first pass this through metamap
        inbox.send(metamap, q);
        List<Concept> concepts = new ArrayList<>();
        try {
            List<Concept> c = (List<Concept>)
                inbox.receive(Duration.ofSeconds(5));
            concepts.addAll(c);
        }
        catch (TimeoutException ex) {
            Logger.warn("Unable to process query with MetMap: "+q);
        }
        return concepts;
    }
    
    public List<Concept> getConcepts (SearchQuery q) {
        List<Concept> concepts = new ArrayList<>();
        if (q.getQuery() != null) {
            concepts = cache.getOrElseUpdate
                (q.cacheKey()+"/concepts", new Callable<List<Concept>> () {
                        public List<Concept> call () {
                            return _getConcepts (q);
                        }
                    });
            Logger.debug("#### "+concepts.size()
                         +" concept(s) found from query "+q);
        }
        return concepts;
    }

    public SearchResult[] getResults (SearchQuery q) {
        // now do the search
        Inbox inbox = Inbox.create(actorSystem);
        for (ActorRef actorRef : indexes)
            inbox.send(actorRef, q);
        
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
        return results.toArray(new SearchResult[0]);
    }

    public SearchResult search (SearchQuery q) {
        Logger.debug("#### Query: "+q);
        q.getConcepts().addAll(getConcepts (q));
        final String key = q.cacheKey()+"/"+q.skip()+"/"+q.top();
        return cache.getOrElseUpdate
            (key, new Callable<SearchResult>() {
                 public SearchResult call () {
                     Logger.debug("Cache missed: "+key);
                     SearchResult result = PubMedIndex.merge(getResults (q));
                     return result.page(q.skip(), q.top());
                 }
             });
    }
    
    public MatchedDoc getDoc (Long pmid) {
        final PMIDQuery q = new PMIDQuery (pmid);
        return cache.getOrElseUpdate
            (q.cacheKey(), new Callable<MatchedDoc>() {
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
                        
                        if (docs.size() > 1) {
                            Logger.warn(pmid+" has multiple ("
                                        +docs.size()+") documents!");

                            Collections.sort(docs);
                        }
                        
                        return docs.isEmpty() ? null : docs.get(0);
                    }
                });
    }

    public SearchResult getDocs (Long... pmids) {
        final PMIDBatchQuery bq = new PMIDBatchQuery (pmids);
        return cache.getOrElseUpdate
            (bq.cacheKey(), new Callable<SearchResult>() {
                    public SearchResult call () {
                        Inbox inbox = Inbox.create(actorSystem);
                        List<SearchResult> results = new ArrayList<>();
                        for (ActorRef actorRef : indexes) {
                            inbox.send(actorRef, bq);
                            try {
                                SearchResult result =
                                    (SearchResult)inbox.receive
                                    (Duration.ofSeconds(5l));
                                results.add(result);
                            }
                            catch (TimeoutException ex) {
                                Logger.error("Unable to receive result from "
                                             +actorRef
                                             +" within alloted time", ex);
                            }
                        }
                        return PubMedIndex.merge
                            (results.toArray(new SearchResult[0]));
                    }
                });
    }
}
