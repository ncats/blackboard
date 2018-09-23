package blackboard.pubmed;

import play.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import blackboard.pubmed.*;
import blackboard.mesh.MeshDb;

public class PubMedIndexBuilder implements AutoCloseable {
    class Builder implements Callable<PubMedIndex> {
        PubMedIndex index;
        int count;

        Builder (File db) throws IOException {
            index = new PubMedIndex (db);
        }

        public PubMedIndex call () throws Exception {
            for (PubMedDoc doc; (doc = queue.take()) != PubMedDoc.EMPTY;) {
                try {
                    index.add(doc);
                    ++count;
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

    public PubMedIndexBuilder () throws IOException {
        this ("pubmed");
    }
    
    public PubMedIndexBuilder (String base) throws IOException {
        this (base, 2);
    }
    
    public PubMedIndexBuilder (String base, int threads) throws IOException {
        es = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; ++i) {
            File db = new File (base+"-"+String.format("%1$02d.db", i+1));
            this.threads.add(es.submit(new Builder (db)));
        }
    }

    public void close () throws Exception {
        for (Future<PubMedIndex> f : threads)
            queue.put(PubMedDoc.EMPTY);
        
        for (Future<PubMedIndex> f : threads) {
            PubMedIndex pmi = f.get();
            pmi.debug();
            pmi.close();
        }
        es.shutdownNow();
    }

    public void build (InputStream is, MeshDb mesh) {
        AtomicInteger count = new AtomicInteger ();
        PubMedSax pms = new PubMedSax (mesh, d -> {
                if (true || count.incrementAndGet() < 1000) {
                    try {
                        queue.put(d);
                    }
                    catch (Exception ex) {
                        Logger.error("Can't queue document "+d.getPMID(), ex);
                    }
                }
                else
                    throw new RuntimeException ("done!");
            });

        try {
            pms.parse(is);
        }
        catch (RuntimeException ex) {
        }
        catch (Exception ex) {
            Logger.error("Can't build index", ex);
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 4) {
            System.err.println
                ("Usage: PubMedIndexBuilder BASE MESHDB THREADS FILES...");
            System.exit(1);
        }

        int threads = Integer.parseInt(argv[2]);
        try (PubMedIndexBuilder pmb = new PubMedIndexBuilder (argv[0], threads);
             MeshDb mesh = new MeshDb (new File (argv[1]))) {
            for (int i = 3; i < argv.length; ++i) {
                pmb.build(new java.util.zip.GZIPInputStream
                          (new FileInputStream (argv[i])), mesh);
            }
        }
    }
}
