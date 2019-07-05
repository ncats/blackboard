package blackboard.pubmed;

import play.Logger;
import play.Application;
import play.inject.Injector;
import play.inject.guice.GuiceApplicationBuilder;
import play.api.Configuration;
import play.inject.ApplicationLifecycle;
import com.typesafe.config.Config;

import blackboard.pubmed.*;
import blackboard.index.pubmed.*;
import static blackboard.index.pubmed.PubMedIndex.*;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.*;
import java.time.Duration;
import java.time.temporal.ChronoField;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;


/*
 * sbt "runMain blackboard.pubmed.PubMedIndexUpdate ARGS..."
 */
public class PubMedIndexUpdater implements AutoCloseable {
    static class Update {
        final Long pmid;
        Update (Long pmid) {
            this.pmid = pmid;
        }
    }

    static class Delete {
        final Long[] input;        
        Delete (Long... input) {
            this.input = input;
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
                .match(Delete.class, this::doDelete)
                .match(Update.class, this::doUpdate)
                .build();
        }

        void doDelete (Delete del) {
            try {
                int dels = pmi.deleteDocs(del.input);
                getSender().tell(dels, getSelf ());
            }
            catch (IOException ex) {
                getSender().tell(ex, getSelf ());
            }
        }

        void doUpdate (Update update) {
            try {
                MatchedDoc[] docs = pmi.getMatchedDocs(update.pmid);
                getSender().tell(docs, getSelf ());
            }
            catch (Exception ex) {
                getSender().tell(ex, getSelf ());
            }
        }
    }
    
    final Application app;
    final List<ActorRef> actors = new ArrayList<>();
    final Inbox inbox;
    final AtomicInteger count = new AtomicInteger ();
    Integer max;
    
    public PubMedIndexUpdater () throws IOException {
        app = new GuiceApplicationBuilder()
            .in(new File("."))
            .build();

        Config conf = app.config().getConfig("app.pubmed");
        if (!conf.hasPath("indexes"))
            throw new IllegalArgumentException
                ("No app.pubmed.indexes property defined!");
        File dir = new File
            (conf.hasPath("base") ? conf.getString("base") : ".");

        ActorSystem actorSystem = app.injector().instanceOf(ActorSystem.class);
        inbox = Inbox.create(actorSystem);
        
        PubMedIndexFactory pmif =
            app.injector().instanceOf(PubMedIndexFactory.class);

        List<String> indexes = conf.getStringList("indexes");
        for (String idx : indexes) {
            File db = new File (dir, idx);
            if (!db.exists()) {
                Logger.warn(idx+": database doesn't exist!");
            }
            else {
                try {
                    ActorRef actorRef = actorSystem.actorOf
                        (PubMedIndexActor.props(pmif, db),
                         PubMedIndexUpdater.class.getName()+"-"+idx);
                    actors.add(actorRef);
                }
                catch (Exception ex) {
                    Logger.error("Can't load database: "+db, ex);
                }
            }
        }
    }

    public void close () throws Exception {
        for (ActorRef actorRef : actors) {
            actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        play.api.Play.stop(app.getWrappedApplication());
    }

    void deleteCitations (Long... citations) {
        Delete del = new Delete (citations);
        for (ActorRef actorRef : actors) {
            inbox.send(actorRef, del);
            try {
                Object res = inbox.receive(Duration.ofSeconds(5l));
                if (res instanceof Throwable) {
                    Logger.error(actorRef+": can't delete citations",
                                 (Throwable)res);
                }
                else {
                    Logger.debug(actorRef+": "+res+" doc(s) deleted!");
                }
            }
            catch (TimeoutException ex) {
                Logger.error("Unable to receive result from "+actorRef
                             +" within alloted time", ex);
            }
        }
    }

    protected PubMedSax createSaxParser () {
        count.set(0);
        PubMedSax pms = new PubMedSax ((s, d) -> {
                boolean cont = false;
                if (max == null || max == 0 || count.intValue() < max) {
                    cont = true;
                    updateDoc (d.pmid);
                    count.incrementAndGet();
                }
                return cont;
            });
        
        return pms;
    }

    void updateDoc (Long pmid) {
        Update upd = new Update (pmid);
        List<MatchedDoc> alldocs = new ArrayList<>();
        for (ActorRef actorRef : actors) {
            inbox.send(actorRef, upd);
        }

        for (ActorRef actorRef : actors) {
            try {
                Object res = inbox.receive(Duration.ofSeconds(5l));
                if (res instanceof Throwable) {
                    Logger.error("Can't update doc "+pmid, (Throwable)res);
                }
                else {
                    MatchedDoc[] docs = (MatchedDoc[])res;
                    if (docs.length > 0) {
                        Logger.debug
                            (actorRef+": "+docs.length+" doc(s) matched!");
                        for (MatchedDoc d : docs)
                            alldocs.add(d);
                    }
                }
            }
            catch (TimeoutException ex) {
                Logger.error("Unable to receive result from "+actorRef
                             +" within alloted time", ex);
            }
        }
        
        if (alldocs.size() > 1) {
            Collections.sort(alldocs);
            Logger.warn(pmid+" has "+alldocs.size()+" instances!");
            for (MatchedDoc d : alldocs) {
                Logger.warn("** deleting "+d.pmid+" "+d.revised);
            }
        }
    }

    public void update (File file) throws Exception {
        PubMedSax pms = createSaxParser ();
        pms.parse(file);
        
        List<Long> citations = pms.getDeleteCitations();
        Logger.debug("## "+count+" doc(s) parsed; "
                     +citations.size()+" delete citations!");
        if (!citations.isEmpty()) {
            deleteCitations (citations.toArray(new Long[0]));
        }
    }
    
    static void usage () {
        System.err.println
            ("Usage: PubMedIndexUpdate [MAX=0] FILES...");
        System.exit(1);
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 1)
            usage ();
        
        try (PubMedIndexUpdater pmiu = new PubMedIndexUpdater ()) {
            for (String a : argv) {
                File f = new File (a);
                if (f.exists())
                    pmiu.update(f);
            }
        }
    }
}
