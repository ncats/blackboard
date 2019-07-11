package blackboard.index;

import java.io.File;

public interface IndexFactory {
    Index get (File dbdir);
}
