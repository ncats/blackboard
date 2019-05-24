package blackboard.pubmed.index;

import play.Logger;
import play.Application;
import play.inject.Injector;
import play.inject.guice.GuiceApplicationBuilder;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;

import blackboard.pubmed.*;
import blackboard.umls.UMLSKSource;
import blackboard.mesh.MeshKSource;
import blackboard.semmed.SemMedDbKSource;

public class PubMedIndexBuilder implements AutoCloseable {
    
    class Builder implements Callable<PubMedIndex> {
        final PubMedIndex index;
        int count;

        Builder (PubMedIndex index) {
            this.index = index;
        }

        public PubMedIndex call () throws Exception {
            for (PubMedDoc doc; (doc = queue.take()) != PubMedDoc.EMPTY;) {
                try {
                    if (addIfAbsent.get()) {
                        boolean added = index.addIfAbsent(doc);
                        if (added)
                            ++count;
                        Logger.debug(Thread.currentThread().getName()
                                     +": added "+doc.getPMID()+"..."+added);
                    }
                    else {
                        index.add(doc);
                        ++count;
                    }
                    Logger.debug(Thread.currentThread().getName()
                                 +": "+doc.getPMID()+"/"+count);
                }
                catch (IOException ex) {
                    Logger.error(Thread.currentThread().getName()
                                 +": not index "+doc.getPMID(), ex);
                }
            }
            Logger.debug(Thread.currentThread().getName()+": "
                         +index.getDbFile()+" "+index.size());
            return index;
        }
    }
    
    final BlockingQueue<PubMedDoc> queue = new ArrayBlockingQueue<>(1000);
    final ExecutorService es;
    final List<Future<PubMedIndex>> threads = new ArrayList<>();
    final PubMedKSource pubmed;
    final Application app;
    final AtomicBoolean addIfAbsent = new AtomicBoolean (false);
    
    public PubMedIndexBuilder () throws IOException {
        this ("pubmed");
    }
    
    public PubMedIndexBuilder (String base) throws IOException {
        this (base, 2);
    }
    
    public PubMedIndexBuilder (String base, int threads)
        throws IOException {
        es = Executors.newFixedThreadPool(threads);
        app = new GuiceApplicationBuilder()
            .in(new File("."))
            .build();
        pubmed = app.injector().instanceOf(PubMedKSource.class);
        for (int i = 0; i < threads; ++i) {
            File db = new File (base+"-"+String.format("%1$02d.db", i+1));
            UMLSKSource umls = app.injector().instanceOf(UMLSKSource.class);
            PubMedIndex index = new PubMedIndex (db);
            index.setMetaMap(umls.getMetaMap());
            index.semmed = app.injector().instanceOf(SemMedDbKSource.class);
            index.mesh = app.injector().instanceOf(MeshKSource.class);
            this.threads.add(es.submit(new Builder (index)));
        }
    }

    public void setAddIfAbsent (boolean b) {
        addIfAbsent.set(b);
    }
    public boolean getAddIfAbsent () { return addIfAbsent.get(); }

    public void close () throws Exception {
        if (es.isShutdown() || es.isTerminated()) {
            Logger.debug("### Instance already closed!");
            return;
        }
        
        for (Future<PubMedIndex> f : threads)
            queue.put(PubMedDoc.EMPTY);

        int total = 0;
        for (Future<PubMedIndex> f : threads) {
            PubMedIndex pmi = f.get();
            total += pmi.size();
            pmi.debug();
            pmi.close();
        }
        Logger.debug("####### "+total+" document(s) indexed!");
        
        es.shutdownNow();
        play.api.Play.stop(app.getWrappedApplication());
    }

    public void buildXml (InputStream is) throws Exception {
        if (es.isShutdown() || es.isTerminated())
            throw new RuntimeException ("Instance has already been closed!");
        
        AtomicInteger count = new AtomicInteger ();
        PubMedSax pms = new PubMedSax (pubmed.mesh, d -> {
                boolean cont = false;
                if (true || count.incrementAndGet() < 1000) {
                    try {
                        queue.put(d);
                        cont = true;
                    }
                    catch (Exception ex) {
                        Logger.error("Can't queue document "+d.getPMID(), ex);
                    }
                }
                return cont;
            });

        pms.parse(is);
    }

    public void build (InputStream is) throws Exception {
        if (es.isShutdown() || es.isTerminated())
            throw new RuntimeException ("Instance has already been closed!");
        
        try (BufferedReader br = new BufferedReader
             (new InputStreamReader (is))) {
            int count = 0;
            for (String line; (line = br.readLine()) != null; ) {
                try {
                    if (true || count < 1000) {
                        Long pmid = Long.parseLong(line);
                        add (pmid);
                        ++count;
                    }
                }
                catch (NumberFormatException ex) {
                    Logger.warn("Bogus PMID: "+line, ex);
                }
                catch (Exception ex) {
                    Logger.error("Can't queue pmid "+line, ex);
                }
            }
            Logger.debug(count+" document(s) queued!");
        }
    }

    public PubMedDoc doc (long pmid) throws Exception {
        return pubmed.getPubMedDoc(pmid);
    }
    
    public void add (PubMedDoc doc) throws Exception {
        queue.put(doc);
    }

    public PubMedDoc add (long pmid) throws Exception {
        PubMedDoc doc = doc (pmid);
        queue.put(doc);
        return doc;
    }

    static void usage () {
        System.err.println
            ("Usage: PubMedIndexBuilder [BASE=pubmed|THREADS=2"
             +"|INPUT=FILE|PMID=FILE] FILES...");
        System.exit(1);
    }

    /*
     * sbt "runMain blackboard.pubmed.index.PubMedIndexBuilder pubmed18n0001.xml.gz"
     */
    public static void main (String[] argv) throws Exception {
        if (argv.length < 1)
            usage ();

        int threads = 2;
        String base = "pubmed";
        List<File> files = new ArrayList<>();
        List<File> pmids = new ArrayList<>();
        for (String a : argv) {
            if (a.startsWith("BASE=")) {
                base = a.substring(5);
                Logger.debug("BASE: "+base);
            }
            else if (a.startsWith("THREADS=")) {
                threads = Integer.parseInt(a.substring(8));
                Logger.debug("THEADS: "+threads);
            }
            else if (a.startsWith("INPUT=")) {
                try (BufferedReader br = new BufferedReader
                     (new InputStreamReader (new FileInputStream
                                             (a.substring(6))));) {
                    for (String line; (line = br.readLine()) != null; ) {
                        File f = new File (line.trim());
                        if (f.exists())
                            files.add(f);
                        else
                            Logger.warn("Skipping unknown file: \""+f+"\"");
                    }
                }
            }
            else if (a.startsWith("PMID=")) {
                File file = new File (a.substring(5));
                if (!file.exists()) {
                    Logger.debug(file+" does not exist!");
                }
                else {
                    pmids.add(file);
                }
            }
            else {
                File f = new File (a);
                if (f.exists())
                    files.add(f);
                else
                    Logger.warn("Skipping unknown file: \""+a+"\"");
            }
        }

        if (files.isEmpty() && pmids.isEmpty())
            usage ();

        Logger.debug("processing "+files.size()+" files!");
        try (final PubMedIndexBuilder pmb =
             new PubMedIndexBuilder (base, threads)) {
            Runtime.getRuntime().addShutdownHook(new Thread () {
                    public void run () {
                        Logger.debug("##### SHUTTING DOWN! ######");
                        try {
                            pmb.close();
                        }
                        catch (Exception ex) {
                            ex.printStackTrace();
                        }
                    }
                });
            
            if (!files.isEmpty()) {
                for (File f : files) {
                    Logger.debug("########## "+f+" #########");
                    long start = System.currentTimeMillis();
                    pmb.buildXml(new java.util.zip.GZIPInputStream
                                 (new FileInputStream (f)));
                    Logger.debug("##### finished "+f+" in "+String.format
                                 ("%1$.3fs", (System.currentTimeMillis()-start)
                                  /1000.));
                }
                Logger.debug("### "+new java.util.Date()+"; "
                             +files.size()+" file(s)");
            }
            
            pmb.setAddIfAbsent(true);
            for (File f : pmids) {
                Logger.debug("########## "+f+" #########");
                try (FileInputStream fis = new FileInputStream (f)) {
                    pmb.build(fis);
                }
                Logger.debug("### "+new java.util.Date()+": "+f);
            }
        }
    }
}
