package blackboard.mesh;

public interface MeshVisitor {
    /*
     * visit each MeSH descriptor; return false to stop traversal
     */
    boolean visit (String ui, String name, String... treeNumbers);
}
