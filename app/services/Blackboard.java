package services;

import java.util.Map;

/**
 * A simple blackboard API
 */
public interface Blackboard extends Iterable<KGraph> {
    KGraph getKGraph (long id);
    long getKGraphCount ();
    KGraph createKGraph (String name, Map<String, Object> properties);
}
