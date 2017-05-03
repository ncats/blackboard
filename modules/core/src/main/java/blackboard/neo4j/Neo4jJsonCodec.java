package blackboard.neo4j;

import java.util.Map;
import java.io.IOException;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ser.ContextualSerializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;

import blackboard.JsonCodec;
import blackboard.KEntity;
import blackboard.BeanViews;

public class Neo4jJsonCodec implements JsonCodec {
    static class KEntitySerializer extends JsonSerializer<KEntity> {
        public KEntitySerializer () {
        }

        public void serialize (KEntity ke, JsonGenerator jgen,
                               SerializerProvider provider)
            throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            provider.defaultSerializeField("id", ke.getId(), jgen);
            provider.defaultSerializeField("type", ke.getType(), jgen);
            provider.defaultSerializeField("name", ke.getName(), jgen);
            for (Map.Entry<String, Object> prop :
                     ke.getProperties().entrySet()) {
                provider.defaultSerializeField
                    (prop.getKey(), prop.getValue(), jgen);
            }
            jgen.writeEndObject();
        }
    }
    
    final ObjectMapper compact, full;
    
    public Neo4jJsonCodec () {
        //SimpleModule module = new SimpleModule ("Neo4j serialization");
        //module.addSerializer(KEntity.class, new KEntitySerializer ());
        //mapper = new ObjectMapper().registerModule(module);
        compact = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            ;
        compact.setConfig(compact.getSerializationConfig()
                          .withView(BeanViews.Compact.class));

        full = new ObjectMapper ()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .setSerializationInclusion(JsonInclude.Include.NON_NULL)
            ;
    }
    
    public ObjectMapper getCompactMapper () { return compact; }
    public ObjectMapper getFullMapper () { return full; }
    public JsonNode toJson (Object value, boolean full) {
        return full ? this.full.valueToTree(value)
            : compact.valueToTree(value);
    }
}
