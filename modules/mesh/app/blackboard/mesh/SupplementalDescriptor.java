package blackboard.mesh;

import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SupplementalDescriptor extends Entry implements CommonDescriptor {
    public Integer freq;
    public String note;
    public List<Descriptor> mapped = new ArrayList<>();
    public List<Descriptor> indexed = new ArrayList<>();
    public List<Concept> concepts = new ArrayList<>();
    public List<Entry> pharm = new ArrayList<>();
    public List<String> sources = new ArrayList<>();
    
    protected SupplementalDescriptor () {}
    protected SupplementalDescriptor (String ui) {
        this (ui, null);
    }
    protected SupplementalDescriptor (String ui, String name) {
        super (ui, name);
    }
    
    public String getUI () { return ui; }
    public String getName () { return name; }
    public List<Concept> getConcepts () { return concepts; }
    public List<Entry> getPharmacologicalActions () { return pharm; }    
}
