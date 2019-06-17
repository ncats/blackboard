package blackboard.disease;

import java.util.List;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DiseaseResult {
    public static final DiseaseResult EMPTY = new DiseaseResult (null);
    
    public final DiseaseQuery query;
    public int total;
    public int status;
    public String message;
    public final List<Disease> diseases = new ArrayList<>();
    
    DiseaseResult (DiseaseQuery query) {
        this.query = query;
    }

    @JsonProperty(value="count")
    public int size () { return diseases.size(); }
    public void add (Disease d) {
        diseases.add(d);
    }
    
    public void setStatus (int status, String message) {
        this.status = status;
        this.message = message;
    }
}
