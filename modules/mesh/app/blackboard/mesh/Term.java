package blackboard.mesh;

public class Term extends Entry {
    protected Term () {}
    protected Term (String ui) {
        this (ui, null);
    }
    protected Term (String ui, String name) {
        super (ui, name);
    }
}
