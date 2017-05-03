package blackboard;

import java.util.Map;
import java.util.Collection;

/**
 * A simple blackboard API
 */
public interface Blackboard extends Iterable<KGraph> {
    KGraph getKGraph (long id);
    long getKGraphCount ();
    KGraph createKGraph (Map<String, Object> properties);

    Collection<String> getNodeTypes ();
    Collection<String> getEdgeTypes ();
    Collection<String> getEvidenceTypes ();
}
