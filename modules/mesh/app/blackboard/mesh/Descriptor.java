package blackboard.mesh;

import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Descriptor extends Qualifier {
    public List<Qualifier> qualifiers = new ArrayList<>();
    @JsonProperty(value="pharmacologicalActions")
    public List<Entry> pharm = new ArrayList<>();
    
    protected Descriptor () {
    }
    protected Descriptor (String ui) {
        this (ui, null);
    }
    protected Descriptor (String ui, String name) {
        super (ui, name);
    }
}
