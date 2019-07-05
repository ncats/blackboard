package blackboard.index.pubmed;

import play.Logger;
import play.inject.ApplicationLifecycle;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import javax.inject.Singleton;
import javax.inject.Inject;

@Singleton
public class DefaultPubMedIndexFactory implements PubMedIndexFactory {
    
    final ConcurrentMap<File, PubMedIndex> indexes =
        new ConcurrentHashMap<>();
    final ReentrantLock lock = new ReentrantLock ();
    final PubMedIndexFactoryFacade factory;

    @Inject
    public DefaultPubMedIndexFactory (PubMedIndexFactoryFacade factory,
                                      ApplicationLifecycle lifecycle) {
        this.factory = factory;
        lifecycle.addStopHook(() -> {
                return CompletableFuture.runAsync
                    (DefaultPubMedIndexFactory.this::close);
            });
    }

    public PubMedIndex get (File db) {
        lock.lock();
        try {
            return indexes.computeIfAbsent
                (db.getCanonicalFile(), file ->  factory.get(file));
        }
        catch (IOException ex) {
            Logger.error("Can't get canonical file: "+db, ex);
            return null;
        }
        finally {
            lock.unlock();
        }
    }

    public void close () {
        lock.lock();
        try {
            for (PubMedIndex index : indexes.values()) {
                try {
                    index.close();
                }
                catch (Exception ex) {
                    Logger.error("Can't close index "+index, ex);
                }
            }
            indexes.clear();
        }
        finally {
            lock.unlock();
        }
    }
}
