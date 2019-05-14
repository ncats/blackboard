package blackboard.semmed;

import java.util.List;
import java.util.ArrayList;

public class Predication {
    public final String subcui;
    public final String subject;
    public final String subtype;
    public final String predicate;
    public final String objcui;
    public final String object;
    public final String objtype;
    public List<Evidence> evidence = new ArrayList<>();
    
    protected Predication (String subcui, String subject, String subtype,
                           String predicate, String objcui, String object,
                           String objtype) {
        this.subcui = subcui;
        this.subject = subject;
        this.subtype = subtype;
        this.predicate = predicate;
        this.objcui = objcui;
        this.object = object;
        this.objtype = objtype;
    }

    public String getOtherCUI (String cui) {
        if (subcui.equals(cui)) return objcui;
        else if (objcui.equals(cui)) return subcui;
        return null;
    }
}
