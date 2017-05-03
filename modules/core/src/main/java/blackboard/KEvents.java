package blackboard;

import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.*;


@Singleton
public class KEvents<T extends KEntity> {
    final ConcurrentMap<Class<T>, List<KEventListener<T>>> listeners =
        new ConcurrentHashMap<>();

    public KEvents () {
    }

    public void subscribe (Class<T> cls, KEventListener<T> listener) {
        List<KEventListener<T>> list = listeners.get(cls);
        if (list == null)
            listeners.put(cls, list = new ArrayList<>());
        list.add(listener);
    }

    public boolean unsubscribe (Class<T> cls, KEventListener<T> listener) {
        List<KEventListener<T>> list = listeners.get(cls);
        return list != null ? list.remove(listener) : false;
    }

    public int fireEvent (Class<T> cls, KEvent<T> kev) {
        List<KEventListener<T>> list = listeners.get(cls);
        int count = -1; 
        if (list != null) {
            count = 0;
            for (KEventListener<T> l : list) {
                l.onEvent(kev);
                ++count;
            }
        }
        return count;
    }
}
