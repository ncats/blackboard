package blackboard.idg.index;

import blackboard.index.Fields;
public interface IDGFields extends Fields {
    public static final String FIELD_TCRDID = "tcrdid";
    public static final String FIELD_GENERIF = "generif";
    public static final String FIELD_IDGTDL = "idgtdl";
    public static final String FACET_IDGTDL = "@idgtdl";
    public static final String FIELD_IDGFAM = "idgfam";
    public static final String FACET_IDGFAM = "@idgfam";
    public static final String FIELD_NOVELTY = "novelty";
    public static final String FIELD_AASEQ = "aaseq";
    public static final String FIELD_GENEID = "geneid";
    public static final String FIELD_CHROMOSOME = "chr";
    public static final String FACET_CHROMOSOME = "@chr";
    public static final String FIELD_DTOID = "dtoid";
    public static final String FIELD_STRINGID = "stringid";
    
    public static final String FIELD_DATASET = "dataset";
    public static final String FACET_DATASET = "@dataset";
    public static final String FIELD_DSCAT = "dscat";
    public static final String FACET_DSCAT = "@dscat"; // dataset category
    public static final String FIELD_DSTYPE = "dstype";
    public static final String FACET_DSTYPE = "@dstype"; // dataset type
}
