package blackboard.umls;

public class MatchedConcept {
    public final Concept concept;
    public final String cui;
    public final String name;
    public final Float score;
    protected MatchedConcept (Concept concept) {
        this (concept, null, null, null);
    }
    protected MatchedConcept (Concept concept, String cui,
                              String name, Float score) {
        this.concept = concept;
        this.cui = cui;
        this.name = name;
        this.score = score;
    }
}
