package services;

public interface KNode extends KBase {
    KNode[] neighbors ();
    int getDegree ();
    int getInDegree ();
    int getOutDegree ();
}
