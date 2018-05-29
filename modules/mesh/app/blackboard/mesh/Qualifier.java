package blackboard.mesh;

import java.util.List;
import java.util.ArrayList;

public class Qualifier extends Entry {
    public String annotation;
    public String abbr;
    public List<Concept> concepts = new ArrayList<>();
    public List<String> treeNumbers = new ArrayList<>();
    
    protected Qualifier () {}
    protected Qualifier (String ui) {
        this (ui, null);
    }
    protected Qualifier (String ui, String name) {
        super (ui, name);
    }
}
