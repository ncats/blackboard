package blackboard.umls;

import java.util.List;
import java.util.ArrayList;

public class Concept {
    final public String cui;
    final public String name;
    final public Source source;
    final public List<Synonym> synonyms = new ArrayList<>(); 
    final public List<SemanticType> semanticTypes = new ArrayList<>();
    final public List<Definition> definitions = new ArrayList<>();
    final public List<Relation> relations = new ArrayList<>();
    protected Concept (String cui, String name, Source source) {
        this.cui = cui;
        this.name = name;
        this.source = source;
    }

    public List<Relation> findRelations (String cui) {
        List<Relation> rels = new ArrayList<>();
        for (Relation r : relations)
            if (cui.equals(r.cui))
                rels.add(r);
        return rels;
    }

    public boolean equals (Object obj) {
        if (obj instanceof Concept) {
            return cui.equals(((Concept)obj).cui);
        }
        return false;
    }
}
