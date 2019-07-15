package blackboard.idg;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonAnyGetter;

public class Entity {
    @JsonIgnore
    public final Long id;
    public String name;
    public String description;
    public final List<String> synonyms = new ArrayList<>();
    public final Map<String, Object> properties = new LinkedHashMap<>();
    
    protected Entity (Long id) {
        if (id == null)
            throw new IllegalArgumentException ("Entity id is null");
        this.id = id;
    }

    public String getType () { return getClass().getSimpleName(); }
    @JsonAnyGetter
    public Map<String, Object> getProperties () { return properties; }
}
