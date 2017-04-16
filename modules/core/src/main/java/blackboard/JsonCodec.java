package blackboard;

import com.fasterxml.jackson.databind.ObjectMapper;

public interface JsonCodec {
    ObjectMapper getObjectMapper ();
}
