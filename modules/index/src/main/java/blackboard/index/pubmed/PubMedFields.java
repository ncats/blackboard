package blackboard.index.pubmed;
import blackboard.index.umls.UMLSFields;

public interface PubMedFields extends UMLSFields {
    public static final String FIELD_YEAR = "year";
    public static final String FACET_YEAR = "@year";
    public static final String FIELD_PMID = "pmid";
    public static final String FACET_UI = "@ui";
    public static final String FIELD_UI = "ui";
    public static final String FACET_TR = "@tr"; // tree number";
    public static final String FIELD_TR = "tr"; // tree number";
    public static final String[] TR_CATEGORIES = {
        "A", // Anatomy
        "B", // Organisms
        "C", // Diseases
        "D", // Chemicals and Drugs
        "E", // Analytical, Diagnostic and Therapeutic
        "F", // Psychiatry and Psychology
        "G", // Phenomena and Processes
        "H", // Disciplines and Occupuations
        "I", // Anthropology..
        "J", // Technology, Industry, ..
        "K", // Humanities
        "L", // Information Science
        "M", // Named Groups
        "N", // Health care
        "V", // Publication Characteristics
        "Z" // Geographicals
    };
    public static final String FIELD_MESH = "mesh";
    public static final String FACET_MESH = "@mesh";
    public static final String FACET_LANG = "@lang";
    public static final String FIELD_LANG = "lang";
    public static final String FIELD_INVESTIGATOR = "investigator";
    public static final String FACET_INVESTIGATOR = "@investigator";
    public static final String FACET_ORCID = "@orcid";
    public static final String FIELD_ORCID = "orcid";
    public static final String FIELD_AFFILIATION = "affiliation";
    public static final String FIELD_PUBTYPE = "pubtype";
    public static final String FACET_PUBTYPE = "@pubtype";
    public static final String FIELD_JOURNAL = "journal";
    public static final String FACET_JOURNAL = "@journal";
    public static final String FIELD_ABSTRACT = "abstract";
    public static final String FIELD_PMC = "pmc";
    public static final String FIELD_DOI = "doi";
    public static final String FIELD_KEYWORD = "keyword";
    public static final String FACET_KEYWORD = "@keyword";
    public static final String FIELD_REFERENCE = "reference";
    public static final String FACET_REFERENCE = "@reference";
    public static final String FACET_PREDICATE = "@predicate";
    public static final String FIELD_GRANTID = "grantid";
    public static final String FACET_GRANTTYPE = "@granttype";
    public static final String FIELD_GRANTAGENCY = "grantagency";
    public static final String FACET_GRANTAGENCY = "@grantagency";
    public static final String FIELD_GRANTCOUNTRY = "grantcountry";
    public static final String FACET_GRANTCOUNTRY = "@grantcountry";
    public static final String FIELD_XML = "xml";
}
