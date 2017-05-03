package blackboard;

import java.util.EventListener;

public interface KEventListener<T extends KEntity> extends EventListener {
    void onEvent (KEvent<T> event);
}
