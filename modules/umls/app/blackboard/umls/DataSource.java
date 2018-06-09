package blackboard.umls;

public class DataSource {
    final public String name;
    final public String version;
    final public String description;

    protected DataSource (String name, String version, String description) {
        this.name = name;
        this.version = version;
        this.description = description;
    }
}
