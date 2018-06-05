package blackboard.mesh;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public interface Mesh {
    String DESC = "Descriptor";
    String QUAL = "Qualifier";
    String SUPP = "SupplementalDescriptor";
    String PA = "PharmacologicalAction";
    String CONCEPT = "Concept";
    String TERM = "Term";
    
    Label DESC_LABEL = Label.label(DESC);
    Label QUAL_LABEL = Label.label(QUAL);
    Label SUPP_LABEL = Label.label(SUPP);
    Label PA_LABEL = Label.label(PA);
    Label TERM_LABEL = Label.label(TERM);
    Label CONCEPT_LABEL = Label.label(CONCEPT);

    RelationshipType PARENT_RELTYPE = RelationshipType.withName("parent");
    RelationshipType CONCEPT_RELTYPE = RelationshipType.withName("concept");
    RelationshipType TERM_RELTYPE = RelationshipType.withName("term");
    RelationshipType SUBSTANCE_RELTYPE = RelationshipType.withName("substance");
    RelationshipType MAPPED_RELTYPE = RelationshipType.withName("mapped");
    RelationshipType INDEXED_RELTYPE = RelationshipType.withName("indexed");
    
    String EXACT_INDEX = "MSH.exact";
    String TEXT_INDEX = "MSH.text";
}
