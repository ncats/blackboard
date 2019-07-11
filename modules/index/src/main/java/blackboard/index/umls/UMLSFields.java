package blackboard.index.umls;
import blackboard.index.Fields;

public interface UMLSFields extends Fields {
    public static final String FACET_CUI = "@cui";
    public static final String FIELD_CUI = "cui";
    public static final String FIELD_CONCEPT = "concept";
    public static final String FACET_CONCEPT = "@concept";
    public static final String FACET_SEMTYPE = "@semtype";
    
    // MetaMap compressed json
    public static final String FIELD_MM_TITLE = "mm_title";
    public static final String FIELD_MM_ABSTRACT = "mm_abstract";
}
