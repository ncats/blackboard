package blackboard.semmed;

public class Evidence {
    final public Long id;
    public Double score;
    final public String pmid;
    final public String context;
    protected Evidence (Long id, String pmid, String context) {
        this.id = id;
        this.pmid = pmid;
        this.context = context;
    }
}
