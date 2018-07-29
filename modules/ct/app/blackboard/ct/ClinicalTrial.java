package blackboard.ct;
import java.util.List;
import java.util.ArrayList;

public class ClinicalTrial {
    public String nctId;
    public String title;
    public String status;
    public String phase;
    public Integer enrollment;
    public List<Intervention> interventions = new ArrayList<>();
    public List<Condition> conditions = new ArrayList<>();

    protected ClinicalTrial () {}
    protected ClinicalTrial (String nctId) { this.nctId = nctId; }
}
