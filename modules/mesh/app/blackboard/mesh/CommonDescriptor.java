package blackboard.mesh;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonIgnore;

public interface CommonDescriptor {
    String getUI ();
    String getName ();
    List<Concept> getConcepts ();
    List<Entry> getPharmacologicalActions ();
    @JsonIgnore
    default Concept getPreferredConcept () {
        for (Concept c : getConcepts ()) {
            if (c.preferred)
                return c;
        }
        return null;
    }
}
