package blackboard.umls;

import java.util.List;
import java.util.ArrayList;

class Definition {
    final public String description;
    final public String source;
    protected Definition (String description, String source) {
        this.description = description;
        this.source = source;
    }
}

class Relation {
    final public String cui;
    final public String type;
    protected Relation (String cui, String type) {
        this.cui = cui;
        this.type = type;
    }
}

class SemanticType {
    final public String id;
    final public String name;
    protected SemanticType (String id, String name) {
        this.id = id;
        this.name = name;
    }
}

class Source {
    final public String name;
    final public String conceptUI; // concept
    final public String descUI; // descriptor
    protected Source (String name, String conceptUI, String descUI) {
        this.name = name;
        this.conceptUI = conceptUI;
        this.descUI = descUI;
    }
}

class Synonym {
    final public String name;
    final public Source source;
    protected Synonym (String name, Source source) {
        this.name = name;
        this.source = source;
    }
}

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
}
