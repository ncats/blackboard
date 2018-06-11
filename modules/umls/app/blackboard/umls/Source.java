package blackboard.umls;

public class Source {
    final public String name;
    final public String conceptUI; // concept
    final public String descUI; // descriptor
    protected Source (String name, String conceptUI, String descUI) {
        this.name = name;
        this.conceptUI = conceptUI;
        this.descUI = descUI;
    }
}
