package blackboard.idg;

import play.Logger;
import play.Application;
import play.inject.Injector;
import play.inject.guice.GuiceApplicationBuilder;
import play.db.NamedDatabase;
import play.db.Database;
import com.typesafe.config.Config;

import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.time.Duration;
import javax.inject.Inject;

import akka.actor.ActorSystem;
import akka.actor.AbstractActor;
import akka.actor.AbstractActor.Receive;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.PoisonPill;
import akka.actor.Inbox;

import blackboard.idg.index.IDGFields;
import blackboard.idg.index.IDGIndex;
import blackboard.idg.index.IDG;
import blackboard.index.IndexFactory;
import blackboard.idg.*;

public class IDGIndexBuilder implements AutoCloseable, IDGFields {
    static class IDGIndexFactory {
        final IndexFactory ifac;
        final Database db;
        
        @Inject
        IDGIndexFactory (@IDG IndexFactory ifac,
                         @NamedDatabase("tcrd") Database db) {
            this.ifac = ifac;
            this.db = db;
        }

        public IDGIndex get (File dbdir) {
            return (IDGIndex) ifac.get(dbdir);
        }
    }

    static class Insert {
        final Target target;
        Insert (Target target) {
            this.target = target;
        }
    }

    static class IDGIndexActor extends AbstractActor {
        static Props props (IDGIndexFactory tif, File db) {
            return Props.create
                (IDGIndexActor.class, () -> new IDGIndexActor (tif, db));
        }

        final IDGIndex index;
        final Database db;
        public IDGIndexActor (IDGIndexFactory tif, File dir) {
            index = tif.get(dir);
            db = tif.db;
        }

        @Override
        public void preStart () {
            index.setConnection(db.getConnection());
            Logger.debug("### "+self ()+ "...initialized!");
        }

        @Override
        public void postStop () {
            try {
                index.getConnection().close();
                Logger.debug("### "+self ()+"...stopped!");
            }
            catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public Receive createReceive () {
            return receiveBuilder()
                .match(Insert.class, this::doInsert)
                .build();
        }

        void doInsert (Insert ins) {
            try {
                index.addTarget(ins.target);
                getSender().tell(ins.target, getSelf ());
            }
            catch (Exception ex) {
                getSender().tell(new Exception
                                 ("Can't insert target "+ins.target.id, ex),
                                 getSelf ());
            }
        }
    }
    
    final Application app;
    final List<ActorRef> actors = new ArrayList<>();
    final Database db;
    final Inbox insertInbox;
    
    public IDGIndexBuilder (File home) throws IOException {
        app = new GuiceApplicationBuilder()
            .in(home)
            .build();

        IDGIndexFactory factory =
            app.injector().instanceOf(IDGIndexFactory.class);
        this.db = factory.db;

        ActorSystem actorSystem = app.injector().instanceOf(ActorSystem.class);
        insertInbox = Inbox.create(actorSystem);
        
        Config conf = app.config().getConfig("app.idg");
        if (!conf.hasPath("indexes"))
            throw new IllegalArgumentException
                ("No app.tcrd.indexes property defined!");
        File dir = new File
            (conf.hasPath("base") ? conf.getString("base") : ".");

        List<String> indexes = conf.getStringList("indexes");
        for (String idx : indexes) {
            File idxdb = new File (dir, idx);
            try {
                ActorRef actorRef = actorSystem.actorOf
                    (IDGIndexActor.props(factory, idxdb),
                     IDGIndexBuilder.class.getName()+"-"+idx);
                actors.add(actorRef);
            }
            catch (Exception ex) {
                Logger.error("Can't build index", ex);
            }
        }
    }

    public void close () throws Exception {
        for (ActorRef actorRef : actors) {
            actorRef.tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
        db.shutdown();
        play.api.Play.stop(app.getWrappedApplication());
    }

    protected Target instrument (Target target, ResultSet rset)
        throws SQLException {
        target.name = rset.getString("name");
        target.idgtdl = Target.TDL.valueOf(rset.getString("tdl"));
        target.gene = rset.getString("sym");
        target.novelty = rset.getDouble("score");
        if (rset.wasNull())
            target.novelty = null;
        target.sequence = rset.getString("seq");
        target.geneid = rset.getInt("geneid");
        if (rset.wasNull())
            target.geneid = null;
        target.uniprot = rset.getString("uniprot");
        target.chr = rset.getString("chr");
        target.dtoid = rset.getString("dtoid");
        target.stringid = rset.getString("stringid");
        return target;
    }
    
    public void build () throws Exception {
        build (0);
    }
    
    public void build (int limits) throws Exception {
        try (Connection con = db.getConnection();
             Statement stm = con.createStatement()) {
            ResultSet rset = stm.executeQuery
                ("select a.target_id,a.protein_id,b.name,"
                 +"b.tdl,b.fam,d.score,c.*\n"
                 +"from t2tc a "
                 +"     join (target b, protein c)\n"
                 +"on (a.target_id = b.id and a.protein_id = c.id)\n"
                 +"left join tinx_novelty d\n"
                 +"    on d.protein_id = a.protein_id \n"
                 //+"where c.id in (18204,862,74,6571)\n"
                 //+"where a.target_id in (875)\n"
                 //+"where c.uniprot = 'P01375'\n"
                 //+"where b.tdl in ('Tclin','Tchem')\n"
                 //+"where b.idgfam = 'kinase'\n"
                 //+" where c.uniprot in ('Q96K76','Q6PEY2')\n"
                 //+"where b.idg2=1\n"
                 +"order by d.score desc, c.id\n"
                 +(limits > 0 ? ("limit "+limits) : ""));
            Random rand = new Random ();
            while (rset.next()) {
                long protid = rset.getLong("protein_id");
                long targetid = rset.getLong("target_id");
                if (rset.wasNull()) {
                    Logger.warn("Not a protein target: "+targetid);
                    continue;
                }
                
                Target target = instrument (new Target (targetid), rset);
                
                ActorRef actorRef = actors.get(rand.nextInt(actors.size()));
                insertInbox.send(actorRef, new Insert (target));
                try {
                    Object res = insertInbox.receive(Duration.ofSeconds(5l));
                    if (res instanceof Throwable) {
                        Throwable t = (Throwable)res;
                        Logger.error(t.getMessage(), t.getCause());
                    }
                    else {
                    }
                }
                catch (TimeoutException ex) {
                    Logger.error("Unable to receive result from "+actorRef
                                 +" within alloted time", ex);
                }
            }
            rset.close();
        }
    }
    
    static void usage () {
        Logger.info("Usage: IDGIndexBuilder [MAX=0] APPHOME");
        System.exit(1);
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 1)
            usage ();

        int i = 0, max = 0;
        if (argv[i].startsWith("MAX=")) {
            try {
                max = Integer.parseInt(argv[i].substring(4));
            }
            catch (NumberFormatException ex) {
                Logger.error("Bogus integer: "+argv[i].substring(4), ex);
            }
            ++i;
        }

        File home = new File (argv[i]);
        if (!home.exists() || !home.isDirectory()) {
            Logger.error(home+": doesn't exist or not a directory!");
            usage ();
        }

        try (IDGIndexBuilder idg = new IDGIndexBuilder (home)) {
            idg.build(max);
        }
    }
}
