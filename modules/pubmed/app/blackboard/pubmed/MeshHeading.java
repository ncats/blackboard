package blackboard.pubmed;

import java.util.List;
import java.util.ArrayList;
import blackboard.mesh.Entry;

public class MeshHeading {
    public final boolean majorTopic;
    public final Entry descriptor;
    public final List<Entry> qualifiers = new ArrayList<>();

    protected MeshHeading (Entry descriptor) {
        this (descriptor, false);
    }
    protected MeshHeading (Entry descriptor, boolean majorTopic) {
        this.descriptor = descriptor;
        this.majorTopic = majorTopic;
    }
}

