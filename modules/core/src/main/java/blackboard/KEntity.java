package blackboard;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonAnyGetter;

public interface KEntity {
    public static final String NAME_P = "name";
    public static final String TYPE_P = "type";
    public static final String CREATED_P = "created";
    
    long getId ();
    String getType ();
    String getName ();
    void set (String prop, Object value);
    Object get (String prop);
    @JsonAnyGetter
    Map<String, Object> getProperties ();
}
