package blackboard.mesh;

import java.util.List;
import java.util.ArrayList;

public class Descriptor extends Qualifier {
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
}
