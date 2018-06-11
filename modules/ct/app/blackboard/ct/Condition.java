package blackboard.ct;

import java.util.Map;
import java.util.TreeMap;
import java.util.List;
import java.util.ArrayList;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAnyGetter;

public class Condition implements Comparable<Condition> {
    public static class Entry {
        public final String ui;
        public final String name;
        public String description;

        Entry (String ui, String name) {
            this.ui = ui;
            this.name = name;
        }
    }
    
    public String name;
    public List<Entry> mesh = new ArrayList<>();
    public List<Entry> umls = new ArrayList<>();
    public Integer count;
    public Boolean rare;

    protected Condition () {}
    protected Condition (String name) {
        this.name = name;
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
