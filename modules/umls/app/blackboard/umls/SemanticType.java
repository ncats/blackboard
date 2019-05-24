package blackboard.umls;

public class SemanticType implements Comparable<SemanticType> {
    public final String id;    
    public final String abbr;
    public final String name;

    protected SemanticType (String id, String abbr, String name) {
        this.id = id;
        this.abbr = abbr;
        this.name = name;
    }

    public int compareTo (SemanticType st) {
        int d = abbr.compareTo(st.abbr);
        if (d == 0)
            d = id.compareTo(st.id);
        return d;
    }

    public boolean equals (Object obj) {
        if (obj instanceof SemanticType) {
            return id.equals(((SemanticType)obj).id);
        }
        return false;
    }
}
