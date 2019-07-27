package blackboard.pubmed;

import play.Logger;
import play.Application;
import play.inject.Injector;
import play.inject.guice.GuiceApplicationBuilder;

import blackboard.pubmed.index.PubMedIndexManager;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


/*
 * sbt "runMain blackboard.pubmed.PubMedIndexUpdate ARGS..."
 */
public class PubMedIndexUpdater {
    static void usage () {
        System.err.println
            ("Usage: PubMedIndexUpdater [MAX=0] FILES...");
        System.exit(1);
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 1)
            usage ();
        
        Application app = new GuiceApplicationBuilder()
            .in(new File("."))
            .build();

        PubMedIndexManager pubmed =
            app.injector().instanceOf(PubMedIndexManager.class);
        
        List<File> files = new ArrayList<>();
        Integer max = null;
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

        List<CompletableFuture> futures = new ArrayList<>();
        for (int i = 0; i < files.size(); ++i) {
            final File f = files.get(i);
            Logger.debug("##### "+String.format("%1$d of %2$d",
                                                i+1, files.size())+": "+f);
            CompletableFuture<Integer> stage =
                pubmed.update(f, true, max).toCompletableFuture();
            stage.thenAcceptAsync(count -> {
                    Logger.debug(f.getName()+": process..."+count);
                });
            futures.add(stage);
        }
        CompletableFuture
            .allOf(futures.toArray(new CompletableFuture[0])).join();
        
        play.api.Play.stop(app.getWrappedApplication());        
    }
}
