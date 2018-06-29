package blackboard.ct;

import java.util.List;
import java.util.ArrayList;

public class EntryMapping {
    public String name;
    public List<Entry> mesh = new ArrayList<>();
    public List<Entry> umls = new ArrayList<>();

    protected EntryMapping () {}
    protected EntryMapping (String name) {
        this.name = name;
    }
}
