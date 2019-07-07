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
    static final int BATCH_SIZE = 2048;
    
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
                .match(Update.class, this::doUpdate)
                .match(Delete.class, this::doDelete)
                .match(Insert.class, this::doInsert)
                .build();
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
    
    final Application app;
    final List<ActorRef> actors = new ArrayList<>();
    final Inbox inbox;
    final PubMedKSource pubmed;
    final AtomicInteger count = new AtomicInteger ();
    final List<PubMedDoc> batch = new ArrayList<>(BATCH_SIZE);
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
        pubmed = app.injector().instanceOf(PubMedKSource.class);
        
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

    public void setMax (int max) {
        this.max = max;
    }

    void deleteCitations (Long... citations) {
        Delete del = new Delete (citations);
        for (ActorRef actorRef : actors) {
            inbox.send(actorRef, del);
            try {
                Object res = inbox.receive(Duration.ofSeconds(5l));
                if (res instanceof Throwable) {
                    Throwable t = (Throwable)res;
                    Logger.error(t.getMessage(), t.getCause());
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
        batch.clear();
        PubMedSax pms = new PubMedSax (pubmed.mesh, (s, d) -> {
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
        
        return pms;
    }

    void updateDocs (PubMedDoc... docs) {
        Update update = new Update (docs);
        for (ActorRef actorRef : actors)
            inbox.send(actorRef, update);

        Map<PubMedDoc, Integer> matched = new HashMap<>();
        for (ActorRef actorRef : actors) {
            try {
                Object res = inbox.receive(Duration.ofSeconds(5l));
                if (res instanceof Throwable) {
                    Throwable t = (Throwable)res;
                    Logger.error(t.getMessage(), t.getCause());
                }
                else if (res instanceof PubMedDoc) {
                    PubMedDoc d = (PubMedDoc)res;
                    Integer c = matched.get(d);
                    matched.put(d, c== null ? 1 : c+1);
                }
                else {
                    docs = (PubMedDoc[])res;
                    for (PubMedDoc d : docs) {
                        Integer c = matched.get(d);
                        matched.put(d, c== null ? 1 : c+1);
                    }
                }
            }
            catch (TimeoutException ex) {
                Logger.error("Unable to receive result "
                             +" within alloted time", ex);
            }
        }

        // now add the new docs
        Random rand = new Random ();
        int ndocs = 0;
        for (Map.Entry<PubMedDoc, Integer> me : matched.entrySet()) {
            //Logger.debug(me.getKey().pmid+"="+me.getValue());
            if (actors.size() == me.getValue()) {
                int pos = rand.nextInt(actors.size());
                ActorRef actorRef = actors.get(pos);
                PubMedDoc doc = me.getKey();
                inbox.send(actorRef, new Insert (doc));
                ++ndocs;
            }
        }

        for (int i = 0; i < ndocs; ++i) {
            try {
                Object res = inbox.receive(Duration.ofSeconds(5l));
                if (res instanceof Throwable) {
                    Throwable t = (Throwable) res;
                    Logger.error(t.getMessage(), t.getCause());
                }
            }
            catch (TimeoutException ex) {
                Logger.error("Unable to receive result "
                                 +" within alloted time", ex);
            }
        }
    }
    
    public void update (File file) throws Exception {
        PubMedSax pms = createSaxParser ();
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
    
    static void usage () {
        System.err.println
            ("Usage: PubMedIndexUpdate [MAX=0] FILES...");
        System.exit(1);
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 1)
            usage ();

        int max = 0;
        try (PubMedIndexUpdater pmiu = new PubMedIndexUpdater ()) {
            List<File> files = new ArrayList<>();
            for (String a : argv) {
                if (a.startsWith("MAX=")) {
                    max = Integer.parseInt(a.substring(4));
                    Logger.debug("MAX: "+max);
                }
                else if (a.startsWith("INPUT=")) {
                    BufferedReader br = new BufferedReader
                        (new FileReader (a.substring(6)));
                    for (String line; (line = br.readLine()) != null; ) {
                        File f = new File (line.trim());
                        if (f.exists())
                            files.add(f);
                    }
                    br.close();
                }
                else {
                    File f = new File (a);
                    if (f.exists())
                        files.add(f);
                }
            }
            
            if (files.isEmpty()) {
                Logger.error("No input file specified!");
                usage ();
            }

            if (max != 0) pmiu.setMax(max);
            for (int i = 0; i < files.size(); ++i) {
                File f = files.get(i);
                Logger.debug("##### "+String.format("%1$d of %2$d",
                                           i+1, files.size())+": "+f);
                pmiu.update(f);
            }
        }
    }
}
