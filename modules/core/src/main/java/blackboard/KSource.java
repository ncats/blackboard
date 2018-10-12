package blackboard;

import java.util.Map;
import java.util.Collection;

/*
 * Knowledge source
 */
public interface KSource {
    void execute (KGraph kgraph, KNode... nodes);
}
