package blackboard;

import java.util.EventObject;

public class KEvent<T extends KEntity> extends EventObject {
    public enum Oper {
        ADD, DELETE, UPDATE
    }
    
    final protected T entity;
    final protected Oper oper;
    
    public KEvent (Object source, T entity, Oper oper) {
        super (source);
        this.entity = entity;
        this.oper = oper;
    }

    public T getEntity () { return entity; }
    public Oper getOper () { return oper; }
}
