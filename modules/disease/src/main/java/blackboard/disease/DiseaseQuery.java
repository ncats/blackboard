package blackboard.disease;

public class DiseaseQuery {
    public final String query;
    public final int skip;
    public final int top;

    DiseaseQuery (String query, int skip, int top) {
        this.query = query;
        this.skip = skip;
        this.top = top;
    }

    public String getQuery () { return query; }
}
