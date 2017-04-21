package blackboard;

import java.util.Map;
import java.util.function.Supplier;
import com.fasterxml.jackson.annotation.JsonAnyGetter;

public interface KEntity {
    public static final String NAME_P = "name";
    public static final String TYPE_P = "type";
    public static final String CREATED_P = "created";
    public static final String DIRECTED_P = "directed";
    public static final String URI_P = "uri";
    
    long getId ();
    String getType ();
    String getName ();
    void put (String prop, Object value);
    void putAll (Map<String, Object> properties);
    void putIfAbsent (String prop, Supplier supplier);
    Object get (String prop);
    @JsonAnyGetter
    Map<String, Object> getProperties ();
}
