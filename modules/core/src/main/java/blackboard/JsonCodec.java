package blackboard;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

public interface JsonCodec {
    ObjectMapper getFullMapper ();
    ObjectMapper getCompactMapper ();
    default JsonNode toJson (Object value) {
        return toJson (value, false);
    }
    JsonNode toJson (Object value, boolean full);
}
