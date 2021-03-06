package blackboard.umls;

public class MatchedConcept {
    public final String cui;
    public final String name;
    public final Float score;
    public final Concept concept;
    protected MatchedConcept (Concept concept) {
        this (concept, concept.cui, concept.name, null);
    }
    protected MatchedConcept (Concept concept, String cui,
                              String name, Float score) {
        this.concept = concept;
        this.cui = cui;
        this.name = name;
        this.score = score;
    }
}
