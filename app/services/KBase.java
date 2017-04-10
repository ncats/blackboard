package services;

public interface KBase {
    long id ();
    String type ();
    String name ();
    void set (String prop, Object value);
    Object get (String prop);
}
