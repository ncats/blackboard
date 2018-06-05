package blackboard.mesh;
import java.io.File;

public interface MeshFactory {
    MeshDb get (File dbdir);
}
