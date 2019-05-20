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
import blackboard.index.Index;
import blackboard.umls.UMLSKSource;
import blackboard.mesh.MeshKSource;
import blackboard.semmed.SemMedDbKSource;

public class PubMedIndexSearcher implements AutoCloseable {
    final Application app;
    final PubMedIndex index;
    
    public PubMedIndexSearcher (File dir) throws IOException {
        app = new GuiceApplicationBuilder()
            .in(new File("."))
            .build();

        PubMedIndexFactory pmif =
            app.injector().instanceOf(PubMedIndexFactory.class);
        index = pmif.get(dir);
    }

    public void close () throws Exception {
        index.close();
        play.api.Play.stop(app.getWrappedApplication());        
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+PubMedIndexSearcher.class.getName()
                               +" DB TERM [FACETS...]");
            System.exit(1);
        }

        File file = new File (argv[0]);
        if (!file.exists()) {
            Logger.error("Database \""+argv[0]+"\" does not exist!");
            System.exit(1);
        }

        try (PubMedIndexSearcher searcher = new PubMedIndexSearcher (file)) {
            Map<String, Object> facets = new LinkedHashMap<>();
            if (argv.length > 2) {
                // format: FIELD:L0/L1/L2...
                for (int i = 2; i < argv.length; ++i) {
                    int pos = argv[i].indexOf(':');
                    if (pos <= 0) {
                        Logger.error("Not a valid facet: "+argv[i]);
                    }
                    else {
                        String name = argv[i].substring(0, pos);
                        String vals = argv[i].substring(pos+1);
                        facets.put(name, vals.split("/"));
                        Logger.debug("facet: "+name+"="+vals);
                    }
                }
            }
            
            PubMedIndex.SearchResult result =
                searcher.index.search(argv[1], facets);
            for (PubMedIndex.MatchedDoc d : result.docs) {
                System.out.println(d.pmid+": "+d.title);
                for (String f : d.fragments)
                    System.out.println("..."+f);
                System.out.println("=== XML ===\n"+d.toXmlString());
            }
            System.out.println("\n********* Full article set *********\n");
            result.exportXML(System.out);
            
            for (Index.Facet f : result.facets)
                Logger.debug(f.toString());
        }
    }
}
