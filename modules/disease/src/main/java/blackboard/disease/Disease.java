package blackboard.disease;

import java.util.*;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.databind.JsonNode;

public class Disease {
    // different fields to retrieve disease name
    static final String[] NAME_FIELDS = {
        "name",
        "NAME",
        "label"
    };

    static final String[] DESC_FIELDS = {
        "Cause",
        "Diagnosis",
        "Inheritance",
        "definition",
        "description",
        "IAO_0000115",
        "DEF",
        "SOS"
    };

    public final Long id; // internal id
    public final String name;
    public String source;
    public final List<String> labels = new ArrayList<>();

    @JsonIgnore public final Map<String, Object> properties = new TreeMap<>();
    @JsonIgnore public final List<Disease> parents = new ArrayList<>();
    @JsonIgnore public final List<Disease> children = new ArrayList<>();

    protected Disease (Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public static Disease getInstance (JsonNode json) {
        JsonNode payload = json.at("/payload/0");
        if (payload.isMissingNode())
            throw new IllegalArgumentException ("Not a valid disease json!");

        JsonNode n = null;
        for (String s : NAME_FIELDS) {
            n = payload.get(s);
            if (n != null)
                break;
        }
        
        if (n == null)
            throw new IllegalArgumentException
                ("Disease json has neither \"name\" nor \"label\"!\n"
                 +payload);
        
        Disease d = new Disease (payload.get("node").asLong(), n.asText());
        for (Iterator<Map.Entry<String, JsonNode>> it = payload.fields();
             it.hasNext(); ) {
            Map.Entry<String, JsonNode> me = it.next();
            JsonNode value = me.getValue();
            if ("source".equals(me.getKey())) {
                // ignore since we already have source field
            }
            else if ("node".equals(me.getKey())
                     || "created".equals(me.getKey())) {
                d.properties.put(me.getKey(), value.asLong());
            }
            else if ("is_rare".equals(me.getKey())) {
                d.properties.put(me.getKey(), value.asBoolean());
            }
            else if (value.isArray()) {
                String[] vals = new String[value.size()];
                for (int i = 0; i < vals.length; ++i)
                    vals[i] = value.get(i).asText();
                d.properties.put(me.getKey(), vals);
            }
            else {
                d.properties.put(me.getKey(), value.asText());
            }
        }
        
        JsonNode labels = json.at("/labels");
        for (int i = 0; i < labels.size(); ++i) {
            String label = labels.get(i).asText();
            if (label.startsWith("S_"))
                d.source = label.substring(2);
            else if ("ENTITY".equalsIgnoreCase(label)
                     || "COMPONENT".equalsIgnoreCase(label)) {
            }
            else {
                d.labels.add(label);
            }
        }
        return d;
    }

    @JsonAnyGetter
    public Map<String, Object> getProperties () {  return properties;  }

    @JsonIgnore
    public String getSummary () {
        for (String f : DESC_FIELDS) {
            Object desc = properties.get(f);
            if (desc != null) {
                if (desc instanceof String[]) {
                    String[] text = (String[])desc;
                    StringBuilder sb = new StringBuilder (text[0]);
                    for (int i = 1; i < text.length; ++i)
                        sb.append("<br>"+text[i]);
                    return sb.toString();
                }
                else {
                    return (String)desc;
                }
            }
        }
        return null;
    }
}
