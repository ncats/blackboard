package blackboard;

public interface KNode extends KEntity {
    KNode[] neighbors (String... tags);
    boolean hasNeighbors (String... tags);
    int getDegree ();
    int getInDegree ();
    int getOutDegree ();
    boolean hasTag (String tag);
    void addTag (String... tags);
    String[] getTags ();
}
