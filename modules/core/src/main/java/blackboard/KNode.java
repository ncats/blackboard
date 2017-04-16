package blackboard;

public interface KNode extends KEntity {
    KNode[] neighbors ();
    int getDegree ();
    int getInDegree ();
    int getOutDegree ();
}
