package blackboard.pubmed.index;

import play.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import com.typesafe.config.Config;
import play.api.Configuration;
import play.inject.ApplicationLifecycle;
import akka.actor.ActorSystem;

@Singleton
public class PubMedIndexManager implements AutoCloseable {
    final List<PubMedIndex> indexes = new ArrayList<>();
    final ActorSystem actorSystem;
    
    @Inject
    public PubMedIndexManager (Configuration config, PubMedIndexFactory pmif,
                               ActorSystem actorSystem,
                               ApplicationLifecycle lifecycle) {
        Config conf = config.underlying().getConfig("app.pubmed");
        String base = ".";
        if (conf.hasPath("base")) {
            base = conf.getString("base");
        }
        
        File dir = new File (base);
        if (!dir.exists()) 
            throw new IllegalArgumentException ("Not a valid base: "+base);
                
        int threads = 2;
        if (conf.hasPath("threads")) {
            threads = conf.getInt("threads");
        }

        if (!conf.hasPath("indexes"))
            throw new IllegalArgumentException
                ("No app.pubmed.indexes property defined!");
        
        List<String> indexes = conf.getStringList("indexes");
        for (String idx : indexes) {
            File db = new File (dir, idx);
            try {
                this.indexes.add(pmif.get(db));
                Logger.debug("#### "+idx+"...loaded!");
            }
            catch (Exception ex) {
                Logger.error("Can't load database: "+db, ex);
            }
        }

        if (this.indexes.isEmpty())
            throw new IllegalArgumentException ("No valid indexes loaded!");

        lifecycle.addStopHook(() -> {
                close ();
                return CompletableFuture.completedFuture(null);
            });

        this.actorSystem = actorSystem;
        Logger.debug("$$$$ "+getClass().getName()
                     +": base="+base+" threads="+threads+" indexes="+indexes);
    }

    public void close () throws Exception {
        for (PubMedIndex pmi : indexes) {
            try {
                pmi.close();
            }
            catch (Exception ex) {
                Logger.error("Can't close database: "+pmi.getDbFile(), ex);
            }
        }
    }
}
