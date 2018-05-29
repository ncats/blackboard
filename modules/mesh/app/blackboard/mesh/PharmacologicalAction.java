package blackboard.mesh;

import java.util.List;
import java.util.ArrayList;

public class PharmacologicalAction extends Entry {
    public List<Entry> substances = new ArrayList<>();
    protected PharmacologicalAction () {}
    protected PharmacologicalAction (String ui, String name) {
        super (ui, name);
    }
}
