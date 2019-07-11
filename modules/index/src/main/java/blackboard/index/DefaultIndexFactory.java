package blackboard.index;

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
public class DefaultIndexFactory implements IndexFactory {
    
    final ConcurrentMap<File, Index> indexes = new ConcurrentHashMap<>();
    final ReentrantLock lock = new ReentrantLock ();
    final IndexFacade facade;

    @Inject
    public DefaultIndexFactory (IndexFacade facade,
                                ApplicationLifecycle lifecycle) {
        this.facade = facade;
        lifecycle.addStopHook(() -> {
                return CompletableFuture.runAsync
                    (DefaultIndexFactory.this::close);
            });
    }

    public Index get (File db) {
        lock.lock();
        try {
            return indexes.computeIfAbsent
                (db.getCanonicalFile(), file ->  facade.get(file));
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
            for (Index index : indexes.values()) {
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
