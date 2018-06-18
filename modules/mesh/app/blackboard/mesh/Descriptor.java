package blackboard.mesh;

import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Descriptor extends Qualifier implements CommonDescriptor {
    public List<Qualifier> qualifiers = new ArrayList<>();
    public List<Entry> pharm = new ArrayList<>();
    
    protected Descriptor () {
    }
    protected Descriptor (String ui) {
        this (ui, null);
    }
    protected Descriptor (String ui, String name) {
        super (ui, name);
    }

    public String getUI () { return ui; }
    public String getName () { return name; }
    public List<Concept> getConcepts () { return concepts; }
    public List<Entry> getPharmacologicalActions () { return pharm; }
}
