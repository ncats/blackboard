package blackboard.idg;
import com.fasterxml.jackson.annotation.JsonIgnore;

public class Entity {
    @JsonIgnore
    public final long id;
    public String name;
    public String description;

    protected Entity (long id) {
        this.id = id;
    }
}
