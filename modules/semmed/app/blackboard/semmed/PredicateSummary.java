package blackboard.semmed;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Set;
import java.util.TreeSet;

public class PredicateSummary {
    public final String concept;
    public final Map<String, Integer> predicates = new TreeMap<>();
    public final Map<String, Integer> semtypes = new TreeMap<>();
    public final Set<Long> pmids = new TreeSet<>();

    protected PredicateSummary (String concept, List<Predication> preds) {
        for (Predication p : preds) {
            Integer c = semtypes.get(p.objtype);
            semtypes.put(p.objtype, c == null ? 1 : c+1);
            
            c = semtypes.get(p.subtype);
            semtypes.put(p.subtype, c == null ? 1 : c+1);

            c = predicates.get(p.predicate);
            predicates.put(p.predicate, c == null ? 1 : c+1);
            
            for (Evidence ev : p.evidence) {
                try {
                    pmids.add(Long.parseLong(ev.pmid));
                }
                catch (NumberFormatException ex) {
                }
            }
        }
        this.concept = concept;        
    }
}
