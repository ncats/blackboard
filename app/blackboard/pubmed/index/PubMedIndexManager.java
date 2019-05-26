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

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;

import static blackboard.pubmed.index.PubMedIndex.*;

@Singleton
public class PubMedIndexManager implements AutoCloseable {
    public static class TextQuery {
        public String field;
        public String query;
        public Map<String, Object> facets = new TreeMap<>();

        TextQuery (String query, Map<String, Object> facets) {
            this.query = query;
            this.facets = facets;
        }
        
        public String toString () {
            return "TextQuery{field="+field+",query="
                +query+",facets="+facets+"}";
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
                .build();
        }

        void doTextSearch (TextQuery q) throws Exception {
            Logger.debug(self()+": searching "+q);

            SearchResult result = null;
            long start = System.currentTimeMillis();
            if (q.field != null) {
                result = pmi.search(q.field, q.query, q.facets); 
            }
            else {
                result = pmi.search(q.query, q.facets);
            }
            Logger.debug(self()+": search completed in "+String.format
                         ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start)));
            
            getSender().tell(result, getSelf ());
        }
    }
    
    final List<ActorRef> indexes = new ArrayList<>();
    final ActorSystem actorSystem;
    
    @Inject
    public PubMedIndexManager (Configuration config, PubMedIndexFactory pmif,
                               ActorSystem actorSystem,
                               ApplicationLifecycle lifecycle) {
        Config conf = config.underlying().getConfig("app.pubmed");
        String base = ".";
        if (conf.hasPath("base")) {
            base = conf.getString("base");
        }
        
        File dir = new File (base);
        if (!dir.exists()) 
            throw new IllegalArgumentException ("Not a valid base: "+base);
                
        if (!conf.hasPath("indexes"))
            throw new IllegalArgumentException
                ("No app.pubmed.indexes property defined!");
        
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

        /*
        if (this.indexes.isEmpty())
            throw new IllegalArgumentException ("No valid indexes loaded!");
        */

        lifecycle.addStopHook(() -> {
                close ();
                return CompletableFuture.completedFuture(null);
            });

        this.actorSystem = actorSystem;
        Logger.debug("$$$$ "+getClass().getName()
                     +": base="+base+" indexes="+indexes);
    }

    public void close () throws Exception {
        for (ActorRef actorRef : indexes) {
            actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        Logger.debug("$$ shutting down "+getClass().getName());
    }

    public SearchResult[] search (String query, Map<String, Object> facets) {
        TextQuery tq = new TextQuery (query, facets);
        Inbox inbox = Inbox.create(actorSystem);
        List<SearchResult> results = new ArrayList<>();
        for (ActorRef actorRef : indexes) {
            inbox.send(actorRef, tq);
            try {
                SearchResult result = (SearchResult)inbox.receive
                    (Duration.ofSeconds(5l));
                if (result.size() > 0)
                    results.add(result);
            }
            catch (TimeoutException ex) {
                Logger.error("Unable to receive result from "+actorRef
                             +" within alloted time", ex);
            }
        }
        return results.toArray(new SearchResult[0]);
    }

    public byte[] getDoc (Long pmid, String format) {
        return new byte[0];
    }
}
