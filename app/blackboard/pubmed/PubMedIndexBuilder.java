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
        Integer port;
        int count;

        Builder (File db) throws IOException {
            this (db, null);
        }
        
        Builder (File db, Integer port) throws IOException {
            index = new PubMedIndex (db);
            if (port != null && port > 1024) {
                index.setMMPort(port);
                this.port = port;
            }
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
                         +index.getDbFile()+" port="+port+" "+index.size());
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
    
    public PubMedIndexBuilder (String base, int threads, Integer... ports)
        throws IOException {
        es = Executors.newFixedThreadPool(threads);
        for (int i = 0; i < threads; ++i) {
            File db = new File (base+"-"+String.format("%1$02d.db", i+1));
            Integer port;
            if (i >= ports.length)
                port = ports.length > 0 ? ports[i % ports.length] : null;
            else
                port = ports[i];
            this.threads.add(es.submit(new Builder (db, port)));
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

    static void usage () {
        System.err.println
            ("Usage: PubMedIndexBuilder "
             +"[BASE=pubmed|THREADS=2|METAMAP=8066..] MESHDB FILES...");
        System.exit(1);
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2)
            usage ();

        List<Integer> ports = new ArrayList<>();
        int threads = 2;
        String base = "pubmed";
        String meshdb = null;
        List<String> files = new ArrayList<>();
        for (String a : argv) {
            if (a.startsWith("BASE=")) {
                base = a.substring(5);
                Logger.debug("BASE: "+base);
            }
            else if (a.startsWith("THREADS=")) {
                threads = Integer.parseInt(a.substring(8));
                Logger.debug("THEADS: "+threads);
            }
            else if (a.startsWith("METAMAP=")) {
                for (String p : a.substring(8).split(",")) {
                    ports.add(Integer.parseInt(p));
                }
                Logger.debug("PORTS: "+ports);
            }
            else if (meshdb == null) {
                meshdb = a;
                Logger.debug("MESH: "+meshdb);
            }
            else {
                files.add(a);
            }
        }

        if (meshdb == null || files.isEmpty())
            usage ();

        Logger.debug("processing "+files.size()+" files!");
        try (PubMedIndexBuilder pmb = new PubMedIndexBuilder
             (base, threads, ports.toArray(new Integer[0]));
             MeshDb mesh = new MeshDb (new File (meshdb))) {
            for (String f : files) {
                pmb.build(new java.util.zip.GZIPInputStream
                          (new FileInputStream (f)), mesh);
            }
        }
    }
}
