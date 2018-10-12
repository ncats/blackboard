package blackboard;

import java.util.Map;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonAnyGetter;

public interface KSourceProvider {
    String getId ();
    String getName ();
    String getVersion ();
    String getUri ();
    @JsonProperty("class")
    String getImplClass ();
    @JsonAnyGetter  Map<String, String> getProperties ();
    @JsonIgnore KSource getKS ();
    @JsonIgnore JsonNode getData ();
}
