package services;

public interface KNode extends KBase {
    KNode[] neighbors ();
    int degree ();
    int in ();
    int out ();
}
