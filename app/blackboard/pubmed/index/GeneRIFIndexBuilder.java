package blackboard.pubmed.index;

import play.Logger;
import play.Application;
import play.inject.Injector;
import play.inject.guice.GuiceApplicationBuilder;
import play.db.NamedDatabase;
import play.db.Database;
import play.inject.ApplicationLifecycle;

import java.io.*;
import java.util.*;
import java.sql.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;
import javax.inject.Named;

import blackboard.pubmed.*;
import blackboard.umls.UMLSKSource;
import blackboard.mesh.MeshDb;

public class GeneRIFIndexBuilder implements AutoCloseable {
    static final GeneRIF POISON = new GeneRIF ();
    static class GeneRIF {
        public final String gene;
        public final Set<Long> pmids = new TreeSet<>();
        public final String text;

        GeneRIF () {
            this (null, null);
        }
        
        GeneRIF (String gene, String text) {
            this.gene = gene;
            this.text = text;
        }
    }
    
    static class GeneRIFSource {
        int total;
        final Database tcrd;
        final BlockingQueue<GeneRIF> queue = new ArrayBlockingQueue<>(1000);
        
        @Inject
        GeneRIFSource (@NamedDatabase("tcrd") Database tcrd,
                       ApplicationLifecycle lifecycle) {

            try (Connection con = tcrd.getConnection()) {
                Statement stm = con.createStatement();
                ResultSet rset = stm.executeQuery
                    ("select count(*) from generif");
                if (rset.next()) {
                    total = rset.getInt(1);
                    Logger.debug
                        ("####### "+total+" generif available!");
                }
            }
            catch (SQLException ex) {
                Logger.error("Can't initialize generif source!", ex);
            }
            
            lifecycle.addStopHook(() -> {
                    tcrd.shutdown();
                    return CompletableFuture.completedFuture(null);
                });
            this.tcrd = tcrd;
        }

        public BlockingQueue<GeneRIF> getQueue () { return queue; }
        public int generate () {
            return generate (0);
        }
        
        public int generate (final int max) {
            int count = 0;
            try (Connection con = tcrd.getConnection()) {
                Statement stm = con.createStatement();
                ResultSet rset = stm.executeQuery
                    ("select a.fam,b.uniprot,b.sym,d.text,d.pubmed_ids "
                     +"from target a, protein b, t2tc c, generif d "
                     +"where a.id=c.target_id and b.id = c.protein_id "
                     +"and d.protein_id = b.id order by d.protein_id");
                while (rset.next() && (max == 0 || count < max)) {
                    String gene = rset.getString("sym");
                    String text = rset.getString("text");
                    String pmids = rset.getString("pubmed_ids");
                    GeneRIF rif = new GeneRIF (gene, text);
                    if (pmids != null) {
                        for (String p : pmids.split("\\|")) {
                            try {
                                rif.pmids.add(Long.parseLong(p));
                            }
                            catch (NumberFormatException ex) {
                            }
                        }
                    }
                    queue.put(rif);
                    Logger.debug(String.format("%1$6d:", count)
                                 +" "+gene+"\t"+text);
                    ++count;
                }
            }
            catch (InterruptedException ex) {
                Logger.error(Thread.currentThread()
                             +": thread interrupted; count = "+count, ex);
            }
            catch (SQLException ex) {
                Logger.error
                    ("Can't retrieve generif due to database error", ex);
            }
            return count;
        }
    }

    class Builder implements Callable<GeneRIFIndex> {
        GeneRIFIndex index;
        int count;

        Builder (File db, UMLSKSource umls) throws IOException {
            index = new GeneRIFIndex (db);
            index.setMetaMap(umls.getMetaMap());
        }

        public GeneRIFIndex call () throws Exception {
            for (GeneRIF rif; (rif = queue.take()) != POISON;) {
                try {
                    for (Long p : rif.pmids) {
                        index.add(pubmed.getPubMedDoc(p), rif.gene, rif.text);
                        ++count;
                    }
                    Logger.debug(Thread.currentThread().getName()
                                 +" "+count+": "+rif.gene+" "+rif.pmids);
                }
                catch (IOException ex) {
                    Logger.error(Thread.currentThread().getName()
                                 +": not index "+rif.gene, ex);
                }
            }
            Logger.debug(Thread.currentThread().getName()+": "
                         +index.getDbFile()+" "+index.size());
            return index;
        }
    }
    
    final PubMedKSource pubmed;
    final Application app;
    final GeneRIFSource generif;
    final BlockingQueue<GeneRIF> queue;
    final String base;
    
    public GeneRIFIndexBuilder () throws IOException {
        this ("pubmed");
    }
    
    public GeneRIFIndexBuilder (String base)
        throws IOException {
        app = new GuiceApplicationBuilder()
            .in(new File("."))
            .build();
        pubmed = app.injector().instanceOf(PubMedKSource.class);
        generif = app.injector().instanceOf(GeneRIFSource.class);
        queue = generif.getQueue();
        this.base = base;
    }

    public void close () throws Exception {
        play.api.Play.stop(app.getWrappedApplication());
    }

    public void build () throws Exception {
        build (2);
    }
    
    public void build (int threads) throws Exception {
        Logger.debug("############### building generif index with "
                     +threads+" threads ####################");
        ExecutorService threadPool = Executors.newFixedThreadPool(threads);
        List<Future<GeneRIFIndex>> futures = new ArrayList<>();
        for (int i = 0; i < threads; ++i) {
            File db = new File (base+"-"+String.format("%1$02d.db", i+1));
            UMLSKSource umls = app.injector().instanceOf(UMLSKSource.class);
            futures.add(threadPool.submit(new Builder (db, umls)));
        }
        generif.generate();
        
        for (Future<GeneRIFIndex> f : futures)
            queue.put(POISON);
        
        for (Future<GeneRIFIndex> f : futures) {
            GeneRIFIndex index = f.get();
            index.debug();
            index.close();
        }
        threadPool.shutdownNow();
    }

    public static void main (String[] argv) throws Exception {
        String base = "generif";
        int threads = 2;

        for (String a : argv) {
            if (a.startsWith("BASE=")) {
                base = a.substring(5);
                Logger.debug("BASE: "+base);
            }
            else if (a.startsWith("THREADS=")) {
                threads = Integer.parseInt(a.substring(8));
                Logger.debug("THEADS: "+threads);
            }
        }
        
        try (final GeneRIFIndexBuilder builder =
             new GeneRIFIndexBuilder (base)) {
            Runtime.getRuntime().addShutdownHook(new Thread () {
                    public void run () {
                        Logger.debug("##### SHUTTING DOWN! ######");
                        try {
                            builder.close();
                        }
                        catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                });
            
            builder.build(threads);
        }
    }
}
