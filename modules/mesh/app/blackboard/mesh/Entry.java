package blackboard.mesh;

import java.util.Date;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Entry implements Comparable<Entry> {
    public Float score;
    public String ui;
    public String name;
    public Date created;
    public Date revised;
    public Date established;
    public Boolean preferred;
    
    protected Entry () {}
    protected Entry (String ui) {
        this (ui, null);
    }
    protected Entry (String ui, String name) {
        this.ui = ui;
        this.name = name;
    }
    
    public boolean equals (Object obj) {
        if (obj instanceof Entry) {
            Entry me =(Entry)obj;
            return ui.equals(me.ui) && name.equals(me.name);
        }
        return false;
    }
    
    public int compareTo (Entry me) {
        int d = ui.compareTo(me.ui);
        if (d == 0)
            d = name.compareTo(me.name);
        return d;
    }

    @JsonProperty(value="@type")
    public String getType () { return getClass().getSimpleName(); }    
}
