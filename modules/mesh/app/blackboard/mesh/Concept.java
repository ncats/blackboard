package blackboard.mesh;

import java.util.List;
import java.util.ArrayList;

public class Concept extends Entry {
    public String casn1;
    public String regno;
    public String note;
    public List<Term> terms = new ArrayList<>();
    public List<Relation> relations = new ArrayList<>();
    public List<String> relatedRegno = new ArrayList<>();
    
    protected Concept () {}
    protected Concept (String ui) {
        this (ui, null);
    }
    protected Concept (String ui, String name) {
        super (ui, name);
    }
}
