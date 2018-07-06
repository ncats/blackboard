package blackboard.semmed;

public class Evidence {
    public Double score;
    final public String pmid;
    final public String context;
    protected Evidence (String pmid, String context) {
        this.pmid = pmid;
        this.context = context;
    }
}
