package blackboard.disease;

public class DiseaseResult {
    public static final DiseaseResult EMPTY = new DiseaseResult (null);
    
    public final DiseaseQuery query;
    public int status;
    public String message;

    DiseaseResult (DiseaseQuery query) {
        this.query = query;
    }
    
    public void setStatus (int status, String message) {
        this.status = status;
        this.message = message;
    }
}
