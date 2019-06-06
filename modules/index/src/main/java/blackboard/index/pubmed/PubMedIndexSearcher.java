package blackboard.index.pubmed;

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
import blackboard.index.Fields;
import blackboard.umls.UMLSKSource;
import blackboard.mesh.MeshKSource;
import blackboard.semmed.SemMedDbKSource;
import static blackboard.index.pubmed.PubMedIndex.*;

public class PubMedIndexSearcher implements AutoCloseable, Fields {
    final Application app;
    final PubMedIndexManager indexManager;

    public PubMedIndexSearcher () throws IOException {
        this (new File("."));
    }
    
    public PubMedIndexSearcher (File base) throws IOException {
        app = new GuiceApplicationBuilder()
            .in(base)
            .build();

        indexManager = app.injector().instanceOf(PubMedIndexManager.class);
    }

    public void close () throws Exception {
        play.api.Play.stop(app.getWrappedApplication());        
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+PubMedIndexSearcher.class.getName()
                               +"BASEDIR TERM [FACETS...]");
            System.exit(1);
        }

        File base = new File (argv[0]);
        if (!base.exists()) {
            Logger.error("BASEDIR \""+argv[0]+"\" does not exist!");
            System.exit(1);
        }

        try (PubMedIndexSearcher searcher = new PubMedIndexSearcher (base)) {
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
            
            SearchResult result =
                searcher.indexManager.search(argv[1], facets);
            for (MatchedDoc d : result.docs) {
                Logger.debug("################# "
                             +d.pmid+": "+String.format("%1$.3f", d.score)
                             +"\n"+d.title);
                for (MatchedFragment f : d.fragments)
                    Logger.debug("..."+f);
                //System.out.println("=== XML ===\n"+d.toXmlString());
            }
            //Logger.debug("\n********* Full article set *********\n");
            //result.exportXML(System.out);
            
            for (Index.Facet f : result.facets)
                Logger.debug(f.toString());

            Logger.debug("####### "+result.size()+" matches!");
        }
    }
}
