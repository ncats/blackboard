package blackboard;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public interface KSourceProvider {
    String getId ();
    String getName ();
    String getVersion ();
    String getUri ();
    @JsonProperty("class")
    String getImplClass ();
    @JsonIgnore
    KSource getKS ();
}
