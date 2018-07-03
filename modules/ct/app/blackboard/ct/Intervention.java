package blackboard.ct;

public class Intervention extends EntryMapping {
    public String unii;
    public String cui; // umls concept
    public String ui; // mesh descriptor

    protected Intervention () {}
    protected Intervention (String unii, String name) {
        super (name);
        this.unii = unii;
    }
}
