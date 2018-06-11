package blackboard.umls;

public class Relation {
    final public String rui; // unique relation id
    final public String cui;
    final public String type;
    final public String attr;
    final public String source;
    protected Relation (String rui, String cui, String type,
                        String attr, String source) {
        this.rui = rui;
        this.cui = cui;
        this.type = type;
        this.attr = attr;
        this.source = source;
    }
}
