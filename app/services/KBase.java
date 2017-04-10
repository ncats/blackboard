package services;

public interface KBase {
    long id ();
    void set (String name, Object value);
    Object get (String name);
}
