package services;

public interface KBase {
    long getId ();
    String getType ();
    String getName ();
    void set (String prop, Object value);
    Object get (String prop);
}
