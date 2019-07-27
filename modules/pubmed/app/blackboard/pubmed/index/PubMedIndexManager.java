package blackboard.pubmed.index;

import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import static java.util.concurrent.CompletableFuture.supplyAsync;

import com.typesafe.config.Config;
import play.api.Configuration;
import play.inject.ApplicationLifecycle;
import play.cache.SyncCacheApi;
import play.cache.AsyncCacheApi;
import play.libs.ws.*;
import play.libs.concurrent.CustomExecutionContext;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;
import akka.pattern.Patterns;
import akka.util.Timeout;
import static akka.dispatch.Futures.sequence;
import static scala.compat.java8.FutureConverters.*;
    
import blackboard.umls.UMLSKSource;
import static blackboard.index.Index.TextQuery;
import blackboard.index.IndexFactory;
import blackboard.index.Index;
import blackboard.index.Fields;
import blackboard.pubmed.*;
import static blackboard.index.Index.TextQuery;
import static blackboard.index.Index.TermVector;
import static blackboard.pubmed.index.PubMedIndex.*;
import blackboard.utils.Util;

import com.fasterxml.jackson.databind.JsonNode;

@Singleton
public class PubMedIndexManager implements AutoCloseable {
    static final int BATCH_SIZE = 2048;
    
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
                .match(FldQuery.class, this::doMetaMap)
                .build();
        }

        void doMetaMap (FldQuery q) throws Exception {
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
                getSender().tell(ex, getSelf ());
            }
        }
    }

    static public class TermVectorQuery {
        SearchQuery query;
        final String field;
        public TermVectorQuery (String field) {
            this.field = field;
        }
        public TermVectorQuery (String field, SearchQuery query) {
            this.field = field;
            this.query = query;
        }
    }

    static class Update {
        final PubMedDoc[] docs;
        Update (PubMedDoc... docs) {
            this.docs = docs;
        }
    }

    static class Delete {
        final Long[] input;        
        Delete (Long... input) {
            this.input = input;
        }
    }

    static class Insert {
        final PubMedDoc doc;
        Insert (PubMedDoc doc) {
            this.doc = doc;
        }
    }
    
    static class PubMedIndexActor extends AbstractActor {
        static Props props (IndexFactory pmif, File db) {
            return Props.create
                (PubMedIndexActor.class, () -> new PubMedIndexActor (pmif, db));
        }
        
        final PubMedIndex pmi;
        public PubMedIndexActor (IndexFactory pmif, File dir) {
            pmi = (PubMedIndex) pmif.get(dir);
            Logger.debug("*** index loaded..."+dir+" "+pmi.size()+" doc(s)!");
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
                .match(TextQuery.class, this::doSearch)
                .match(PMIDQuery.class, this::doPMIDSearch)
                .match(PMIDBatchQuery.class, this::doSearch)
                .match(FldQuery.class, this::doSearch)
                .match(FacetQuery.class, this::doFacetSearch)
                .match(TermVectorQuery.class, this::doTermVector)
                .match(Update.class, this::doUpdate)
                .match(Delete.class, this::doDelete)
                .match(Insert.class, this::doInsert)
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

        void doTermVector (TermVectorQuery tvq) throws Exception {
            Logger.debug(self()+": term vector "+tvq.field);
            long start = System.currentTimeMillis();
            TermVector tv = pmi.termVector(tvq.field, tvq.query);
            Logger.debug(self()+": term vector for \""+tvq.field
                         +"\" completed in "+String.format
                         ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start)));
            getSender().tell(tv, getSelf ());
        }

        void doDelete (Delete del) {
            try {
                int dels = pmi.deleteDocs(del.input);
                getSender().tell(dels, getSelf ());
            }
            catch (IOException ex) {
                getSender().tell(new Exception
                                 (getSelf()+": Can't delete docs" , ex),
                                 getSelf ());
            }
        }

        void doUpdate (Update update) {
            PubMedDoc[] docs = update.docs;
            try {
                int dels = pmi.deleteDocsIfOlderThan(docs);
                PubMedDoc[] newDocs = pmi.checkDocsIfNewerThan(docs);
                getSender().tell(newDocs, getSelf ());
            }
            catch (Exception ex) {
                getSender().tell(new Exception
                                 (getSelf()+": Can't update docs", ex),
                                 getSelf ());
            }
        }

        void doInsert (Insert insert) {
            PubMedDoc doc = insert.doc;
            try {
                pmi.add(doc);
                getSender().tell(doc, getSelf ());
            }
            catch (IOException ex) {
                getSender().tell
                    (new Exception (getSelf()+": Can't insert doc "+doc.pmid,
                                    ex), getSelf ());
            }
        }
    }

    class IndexUpdate {
        AtomicInteger count = new AtomicInteger ();
        List<PubMedDoc> batch = new ArrayList<>(BATCH_SIZE);

        IndexUpdate (File file, Integer max) throws Exception {
            PubMedSax pms = createSaxParser (max);
            pms.parse(file);
            
            if (!batch.isEmpty()) {
                updateDocs (batch.toArray(new PubMedDoc[0]));
            }
            
            List<Long> citations = pms.getDeleteCitations();
            Logger.debug("## "+count+" doc(s) parsed; "
                         +citations.size()+" delete citations!");
            if (!citations.isEmpty()) {
                deleteCitations (citations.toArray(new Long[0]));
            }
        }

        PubMedSax createSaxParser (final Integer max) {
            return new PubMedSax (pubmed.mesh, (s, d) -> {
                    if (max == null || max == 0 || count.intValue() < max) {
                        if (batch.size() == BATCH_SIZE) {
                            updateDocs (batch.toArray(new PubMedDoc[0]));
                            batch.clear();
                        }
                        batch.add(d);
                    }
                    count.incrementAndGet();
                    return true;
                });
        }
    } // IndexUpdate
    
    final List<ActorRef> indexes = new ArrayList<>();
    final ActorRef metamap;
    final ActorSystem actorSystem;
    final int maxTimeout, maxTries, maxHits;
    final Timeout timeout;
    final AsyncCacheApi cache;
    final UMLSKSource umls;
    final PubMedKSource pubmed;
    final CustomExecutionContext pmec;
    protected SearchResult defaultAllFacets;
    
    @Inject
    public PubMedIndexManager (Configuration config, @PubMed IndexFactory ifac,
                               UMLSKSource umls, PubMedKSource pubmed,
                               ActorSystem actorSystem, AsyncCacheApi cache,
                               PubMedExecutionContext pmec,
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
        timeout = new Timeout (maxTimeout, TimeUnit.SECONDS);
        maxTries = conf.hasPath("max-tries") ? conf.getInt("max-tries") : 5;
        maxHits = conf.hasPath("max-hits") ? conf.getInt("max-hits") : 20;

        String dispatcher = conf.getString("dispatcher");
        if (dispatcher == null)
            dispatcher = "pubmed-dispatcher";
        
        this.pmec = pmec;
        
        List<String> indexes = conf.getStringList("indexes");
        for (String idx : indexes) {
            File db = new File (dir, idx);
            try {
                ActorRef actorRef = actorSystem.actorOf
                    (PubMedIndexActor.props(ifac, db)
                     .withDispatcher(dispatcher),
                     getClass().getName()+"-"+idx);
                this.indexes.add(actorRef);
            }
            catch (Exception ex) {
                Logger.error("Can't load database: "+db, ex);
            }
        }

        metamap = actorSystem.actorOf
            (MetaMapActor.props(umls).withDispatcher(dispatcher));
        lifecycle.addStopHook(() -> {
                return CompletableFuture.runAsync
                    (() -> {
                        try {
                            close ();
                        }
                        catch (Exception ex) {
                            Logger.error("Can't close index manager", ex);
                        }
                    });
            });

        this.umls = umls;
        this.pubmed = pubmed;
        this.actorSystem = actorSystem;
        this.cache = cache;
        
        Logger.debug("$$$$ "+getClass().getName()
                     +": base="+dir
                     +" max-hits="+maxHits
                     +" max-timeout="+maxTimeout
                     +" indexes="+indexes
                     +" dispatcher="+dispatcher);
    }

    public void close () throws Exception {
        metamap.tell(PoisonPill.getInstance(), ActorRef.noSender());
        for (ActorRef actorRef : indexes) {
            actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        Logger.debug("$$ shutting down "+this);
    }

    public CompletionStage<SearchResult> search
        (String query, Map<String, Object> facets) {
        return search (query, facets, 0, maxHits);
    }

    public CompletionStage<SearchResult> search
        (Map<String, Object> facets, int skip, int top) {
        return search (null, facets, skip, top);
    }

    public CompletionStage<SearchResult> search
        (String query, Map<String, Object> facets, int skip, int top) {
        return search (null, query, facets, skip, top, 1);
    }
    
    public CompletionStage<SearchResult> search (String field, String query,
                                                 Map<String, Object> facets,
                                                 int skip, int top, int slop) {
        final TextQuery tq = new TextQuery (field, query, facets);
        tq.skip = skip;
        tq.top = top;
        tq.slop = slop;
        final String key = "PubMedIndexManager/search/"+tq.cacheKey();
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                return search (tq);
            });
    }

    public synchronized CompletionStage<SearchResult> facets () {
        if (defaultAllFacets == null) {
            try {
                defaultAllFacets = facets(100).toCompletableFuture().get();
                Logger.debug("** full facets initialized! **");
            }
            catch (Exception ex) {
                Logger.error("** full facets not initialized **", ex);
            }
        }
        return CompletableFuture.completedFuture(defaultAllFacets);
    }

    public CompletionStage<SearchResult> facets (String facet, String value) {
        return facets (new FacetQuery (facet, value));
    }
    
    public CompletionStage<SearchResult> facets (int fdim) {
        return facets (new FacetQuery (fdim));
    }

    public CompletionStage<SearchResult> facets (Map<String, Object> facets) {
        return facets (new FacetQuery (facets));
    }

    public CompletionStage<SearchResult> facets (SearchQuery q) {
        Logger.debug("#### Facets: "+q);
        final String key = "PubMedIndexManager/facets/"+q.cacheKey();
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                return getResultsAsync(q)
                    .thenApplyAsync(results -> PubMedIndex.merge(results),
                                    pmec);
            });
    }
    
    public CompletionStage<List<Concept>> getConcepts (SearchQuery q) {
        CompletionStage<List<Concept>> stage = null;
        if (q.getQuery() != null) {
            final String key = "PubMedIndexManager/concepts/"+q.cacheKey();
            stage = cache.getOrElseUpdate(key, () -> {
                    Logger.debug("Cache missed: "+key);
                    return toJava(Patterns.ask(metamap, q, timeout))
                    .toCompletableFuture()
                    .thenApplyAsync(concepts -> (List<Concept>)concepts, pmec);
                });
        }
        else {
            stage = supplyAsync (()->new ArrayList<>());
        }
        return stage;
    }

    public CompletionStage<SearchResult[]> __getResults (SearchQuery q) {
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
        return supplyAsync (()->results.toArray(new SearchResult[0]), pmec);
    }

    public SearchResult[] getResults (SearchQuery q) {
        List<CompletableFuture> futures = new ArrayList<>();
        for (ActorRef actorRef : indexes) {
            scala.concurrent.Future future = Patterns.ask(actorRef, q, timeout);
            futures.add(toJava(future).toCompletableFuture());
        }

        // wait for completion...
        CompletableFuture.allOf
            (futures.toArray(new CompletableFuture[0])).join();
        List<SearchResult> results = futures.stream()
            .map(f -> {
                    try {
                        return (SearchResult)f.get();
                    }
                    catch (Exception ex) {
                        throw new RuntimeException (ex);
                    }
                }).collect(Collectors.toList());
            
        return results.toArray(new SearchResult[0]);
    }

    public CompletionStage<SearchResult[]> getResultsAsync (SearchQuery q) {
        return supplyAsync (()->getResults (q), pmec);
    }
    
    public CompletionStage<SearchResult> search (SearchQuery q) {
        Logger.debug("#### Query: "+q);
        final String key = "PubMedIndexManager/"+q.cacheKey();
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                
                return getConcepts(q)
                    .thenAcceptAsync(concepts ->
                                     q.getConcepts().addAll(concepts), pmec)
                    .thenApplyAsync(none -> getResults(q), pmec)
                    .thenApplyAsync(results -> PubMedIndex.merge(results), pmec)
                    .thenApplyAsync(result -> result.page(q.skip(), q.top()),
                                    pmec);
            });
    }

    public CompletionStage<MatchedDoc> getDoc (PMIDQuery q) {
        Inbox inbox = Inbox.create(actorSystem);
        List<MatchedDoc> docs = new ArrayList<>();
        for (ActorRef actorRef : indexes) {
            inbox.send(actorRef, q);
            try {
                MatchedDoc doc = (MatchedDoc)inbox.receive
                    (Duration.ofSeconds(maxTimeout));
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
            Logger.warn(q.pmid+" has multiple ("
                        +docs.size()+") documents!");

            Collections.sort(docs);
        }
                        
        return supplyAsync (() -> docs.isEmpty() ? null : docs.get(0), pmec);
    }
    
    public CompletionStage<MatchedDoc> getDoc (Long pmid) {
        final PMIDQuery q = new PMIDQuery (pmid);
        final String key = "PubMedIndexManager/doc/"+pmid;
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                return getDoc (q);
            });
    }

    public CompletionStage<SearchResult> getDocs (PMIDBatchQuery bq) {
        Inbox inbox = Inbox.create(actorSystem);
        List<SearchResult> results = new ArrayList<>();
        for (ActorRef actorRef : indexes) {
            inbox.send(actorRef, bq);
            try {
                SearchResult result =
                    (SearchResult)inbox.receive(Duration.ofSeconds(maxTimeout));
                results.add(result);
            }
            catch (TimeoutException ex) {
                Logger.error("Unable to receive result from "
                             +actorRef
                             +" within alloted time", ex);
            }
        }
        
        return supplyAsync (() -> PubMedIndex.merge
                            (results.toArray(new SearchResult[0])), pmec);
    }
    
    public CompletionStage<SearchResult> getDocs (Long... pmids) {
        if (pmids.length == 0)
            return supplyAsync (() -> PubMedIndex.EMPTY_RESULT);
        
        final PMIDBatchQuery bq = new PMIDBatchQuery (pmids);
        final String key = "PubMedIndexManager/docs/"+bq.cacheKey();
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                return getDocs (bq);
            });
    }

    public CompletionStage<TermVector> getTermVector (TermVectorQuery tvq) {
        List<CompletableFuture> futures = new ArrayList<>();
        for (ActorRef actorRef : indexes) {
            scala.concurrent.Future future =
                Patterns.ask(actorRef, tvq, timeout);
            futures.add(toJava(future).toCompletableFuture());
        }

        // wait for completion...
        CompletableFuture.allOf
            (futures.toArray(new CompletableFuture[0])).join();

        return supplyAsync (() -> {
                final TermVector vector = Index.createTermVector(tvq.field);
                for (CompletableFuture f : futures) {
                    try {
                        TermVector tv = (TermVector)f.get();
                        vector.add(tv);
                    }
                    catch (Exception ex) {
                        throw new RuntimeException (ex);
                    }
                }
                return vector;
            }, pmec);
    }
    
    public CompletionStage<TermVector> getTermVector (final String field,
                                                      final SearchQuery query) {
        final TermVectorQuery tvq = new TermVectorQuery (field, query);
        final String key = "PubMedIndexManager/termVector/"+field
            +(query != null ? "/"+query.cacheKey():"");
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                return getTermVector (tvq);
            });
    }
    
    public CompletionStage<TermVector> getTermVector (final String field) {
        return getTermVector (field, null);
    }

    protected void updateDocs (PubMedDoc... docs) {
        Update update = new Update (docs);
        List<CompletableFuture> futures = new ArrayList<>();
        for (ActorRef actorRef : indexes) {
            scala.concurrent.Future future =
                Patterns.ask(actorRef, update, timeout);
            futures.add(toJava(future).toCompletableFuture());
        }
        
        // wait for completion...
        CompletableFuture.allOf
            (futures.toArray(new CompletableFuture[0])).join();

        final Map<PubMedDoc, Integer> matched = new HashMap<>();
        for (CompletableFuture f : futures) {
            try {
                Object res = f.get();
                if (res instanceof Throwable) {
                    Throwable t = (Throwable)res;
                    Logger.error(t.getMessage(), t.getCause());
                }
                else {
                    docs = (PubMedDoc[])res;
                    for (PubMedDoc d : docs) {
                        Integer c = matched.get(d);
                        matched.put(d, c== null ? 1 : c+1);
                    }
                }
            }
            catch (Exception ex) {
                Logger.error("Failed to update docs", ex);
            }
        }

        // now add the new docs
        Random rand = new Random ();
        futures.clear();
        for (Map.Entry<PubMedDoc, Integer> me : matched.entrySet()) {
            //Logger.debug(me.getKey().pmid+"="+me.getValue());
            if (indexes.size() == me.getValue()) {
                int pos = rand.nextInt(indexes.size());
                ActorRef actorRef = indexes.get(pos);
                PubMedDoc doc = me.getKey();
                scala.concurrent.Future future =
                    Patterns.ask(actorRef, new Insert (doc), timeout);
                futures.add(toJava(future).toCompletableFuture());
            }
        }
        
        CompletableFuture.allOf
            (futures.toArray(new CompletableFuture[0])).join();
        for (CompletableFuture f : futures) {
            try {
                Object res = f.get();
                if (res instanceof Throwable) {
                    Throwable t = (Throwable) res;
                    Logger.error(t.getMessage(), t.getCause());
                }
            }
            catch (Exception ex) {
                Logger.error("Unable to update documents!", ex);
            }
        }
    }

    protected void deleteCitations (Long... citations) {
        Delete del = new Delete (citations);
        List<CompletableFuture> futures = new ArrayList<>();        
        for (ActorRef actorRef : indexes) {
            scala.concurrent.Future future =
                Patterns.ask(actorRef, del, timeout);
            futures.add(toJava(future).toCompletableFuture());
        }

        CompletableFuture.allOf
            (futures.toArray(new CompletableFuture[0])).join();
        for (CompletableFuture f : futures) {
            try {
                Object res = f.get();
                if (res instanceof Throwable) {
                    Throwable t = (Throwable)res;
                    Logger.error(t.getMessage(), t.getCause());
                }
                else {
                    Logger.debug("** "+res+" doc(s) deleted!");
                }
            }
            catch (Exception ex) {
                Logger.error("Unable to delete citations!", ex);
            }
        }
    }

    public CompletionStage<Integer> update (File file) throws Exception {
        return update (file, true, null);
    }

    public CompletionStage<Integer> update (File file, boolean checkfile)
        throws Exception {
        return update (file, checkfile, null);
    }
    
    public CompletionStage<Integer> update (File file, boolean checkfile,
                                            Integer max) throws Exception {
        if (checkfile) {
            // only load this file if it's not already loaded
            String source = file.getName();
            int pos = source.indexOf('.');
            if (pos > 0)
                source = source.substring(0, pos);
            FldQuery tq = new FldQuery (FIELD_FILE, source);
            tq.top = 1;
            return search(tq).thenApplyAsync(result -> {
                    try {
                        if (result.total == 0)
                            return new IndexUpdate(file, max).count.get();
                    }
                    catch (Exception ex) {
                        throw new RuntimeException (ex);
                    }
                    Logger.warn(file.getName()+" is already indexed!");
                    return 0;
                }, pmec);
        }
        return supplyAsync (()->{
                try {
                    return new IndexUpdate(file, max).count.get();
                }
                catch (Exception ex) {
                    throw new RuntimeException (ex);
                }
            }, pmec);
    }
}
