package blackboard.umls;

import java.util.List;
import java.util.ArrayList;

public class MatchedConcepts {
    public final boolean inexact;
    public final List<Concept> concepts = new ArrayList<>();
    protected MatchedConcepts (boolean inexact) {
        this.inexact = inexact;
    }
    public int getSize () { return concepts.size(); }
}
