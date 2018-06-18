package blackboard.mesh;
import java.util.List;

public interface CommonDescriptor {
    String getUI ();
    String getName ();
    List<Concept> getConcepts ();
    List<Entry> getPharmacologicalActions ();
}
