package blackboard.semmed;

import java.util.List;
import java.util.ArrayList;

public class Predication {
    public final String subject;
    public final String subtype;
    public final String predicate;
    public final String object;
    public final String objtype;
    public List<Evidence> evidence = new ArrayList<>();
    
    protected Predication (String subject, String subtype, String predicate,
                           String object, String objtype) {
        this.subject = subject;
        this.subtype = subtype;
        this.predicate = predicate;
        this.object = object;
        this.objtype = objtype;
    }
}
