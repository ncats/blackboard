package blackboard.umls;

public class Synonym {
    final public String name;
    final public Source source;
    protected Synonym (String name, Source source) {
        this.name = name;
        this.source = source;
    }
}
