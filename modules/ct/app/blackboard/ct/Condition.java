package blackboard.ct;

import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAnyGetter;

public class Condition extends EntryMapping implements Comparable<Condition> {
    public Integer count;
    public Boolean rare;

    protected Condition () {}
    protected Condition (String name) {
        super (name);
    }

    public boolean equals (Object obj) {
        if (obj instanceof Condition) {
            Condition cond = (Condition)obj;
            return name.equals(cond.name);
        }
        return false;
    }

    public int compareTo (Condition cond) {
        int d = cond.count - count;
        if (d == 0)
            d = name.compareTo(cond.name);
        return d;
    }
}
