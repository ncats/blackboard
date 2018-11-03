package blackboard.schemaorg;

import java.util.List;
import com.google.schemaorg.*;
import com.google.schemaorg.core.*;
import com.google.gson.JsonIOException;

public class SchemaOrgFactory {
    protected SchemaOrgFactory () {
    }

    public static MedicalCondition createSampleMedicalCondition ()
        throws Exception {
        MedicalCondition object =
            CoreFactory.newMedicalConditionBuilder()
            .addJsonLdContext(JsonLdFactory.newContextBuilder()
                              .setBase("https://blackboard.ncats.io/").build())
            .setJsonLdId("GARD:8233")
            .addCode(CoreFactory.newMedicalCodeBuilder()
                     .addCodingSystem("GARD").addCodeValue("8233"))
            .addCode(CoreFactory.newMedicalCodeBuilder()
                     .addCodingSystem("ORPHANET").addCodeValue("355"))
            .addCode(CoreFactory.newMedicalCodeBuilder()
                     .addCodingSystem("UMLS").addCodeValue("C0017205"))
            .addCode(CoreFactory.newMedicalCodeBuilder()
                     .addCodingSystem("MSH").addCodeValue("D005776"))
            .addCode(CoreFactory.newMedicalCodeBuilder()
                     .addCodingSystem("DOID").addCodeValue("1926"))
            .addName("Gaucher disease")
            .addProperty("isRare", BooleanEnum.TRUE)
            .addAlternateName("Acute cerebral Gaucher disease")
            .addAlternateName("Cerebroside lipidosis syndrome")
            .addAlternateName("Gaucher splenomegaly")
            .addAlternateName("Sphingolipidosis 1")
            .addAlternateName("Glucocerebrosidosis")
            .addAlternateName("Glucosylceramidase deficiency")
            .addAlternateName("Glucosyl cerebroside lipidosis")
            .addAlternateName("Kerasin lipoidosis")
            .addAlternateName("Kerasin thesaurismosis")
            .addDescription("An inherited lysosomal storage disease caused by deficiency of the enzyme glucocerebrosidase. It results in the accumulation of a fatty substance called glucocerebroside in mononuclear cells in the bone marrow, liver, spleen, brain, and kidneys. Signs and symptoms include hepatomegaly, splenomegaly, neurologic disorders, lymphadenopathy, skeletal disorders, anemia and thrombocytopenia. [NCI]")
            .addSignOrSymptom(CoreFactory.newMedicalSignOrSymptomBuilder()
                              .addCode(CoreFactory.newMedicalCodeBuilder()
                                       .addCodingSystem("HPO")
                                       .addCodeValue("HP:0001903"))
                              .addName("Anemia")
                              .addProperty("frequency", "80%-99%"))
            .addSignOrSymptom(CoreFactory.newMedicalSignOrSymptomBuilder()
                              .addCode(CoreFactory.newMedicalCodeBuilder()
                                       .addCodingSystem("HPO")
                                       .addCodeValue("HP:0012378"))
                              .addName("Fatigue")
                              .addProperty("frequency", "80%-99%"))
            .addSignOrSymptom(CoreFactory.newMedicalSignOrSymptomBuilder()
                              .addCode(CoreFactory.newMedicalCodeBuilder()
                                       .addCodingSystem("HPO")
                                       .addCodeValue("HP:0002240"))
                              .addName("Hepatomegaly")
                              .addProperty("frequency", "80%-99%"))
            .addSignOrSymptom(CoreFactory.newMedicalSignOrSymptomBuilder()
                              .addCode(CoreFactory.newMedicalCodeBuilder()
                                       .addCodingSystem("HPO")
                                       .addCodeValue("HP:0001744"))
                              .addName("Splenomegaly")
                              .addProperty("frequency", "80%-99%"))
            .addSignOrSymptom(CoreFactory.newMedicalSignOrSymptomBuilder()
                              .addCode(CoreFactory.newMedicalCodeBuilder()
                                       .addCodingSystem("HPO")
                                       .addCodeValue("HP:0002027"))
                              .addName("Abdominal pain")
                              .addProperty("frequency", "30%-79%"))
            .addSignOrSymptom(CoreFactory.newMedicalSignOrSymptomBuilder()
                              .addCode(CoreFactory.newMedicalCodeBuilder()
                                       .addCodingSystem("HPO")
                                       .addCodeValue("HP:0002829"))
                              .addName("Arthralgia")
                              .addProperty("frequency", "30%-79%"))
            .addProperty("organizations", CoreFactory.newOrganizationBuilder()
                         .addAddress(CoreFactory.newPostalAddressBuilder()
                                     .addName("Center for Jewish Genetics")
                                     .addStreetAddress("30 South Wells St.")
                                     .addAddressLocality("Chicago")
                                     .addAddressRegion("IL")
                                     .addPostalCode("60606")
                                     .addAddressCountry("USA")
                                     .addTelephone("312-855-3295")
                                     .addUrl("https://www.jewishgenetics.org/")
                                     .addEmail("jewishgeneticsctr@juf.org")))
            .addProperty("organizations",CoreFactory.newOrganizationBuilder()
                         .addAddress(CoreFactory.newPostalAddressBuilder()
                                     .addName("Jewish Genetic Disease Consortium (JGDC)")
                                     .addStreetAddress("450 West End Ave., 6A")
                                     .addAddressLocality("New York")
                                     .addAddressRegion("NY")
                                     .addPostalCode("10024")
                                     .addAddressCountry("USA")
                                     .addTelephone("855-642-6900")
                                     .addTelephone("866-370-GENE (4363)")
                                     .addFaxNumber("212-873-7892")
                                     .addUrl("http://www.JewishGeneticDiseases.org")
                                     .addEmail("info@jewishgeneticdiseases.org")))
            .addPossibleTreatment
            (CoreFactory.newMedicalTherapyBuilder()
             .addCode(CoreFactory.newMedicalCodeBuilder()
                      .addCodingSystem("UNII").addCodeValue("Q6U6J48BWY"))
             .addName("Imiglucerase")
             .addName("Cerezyme®")
             .addUrl("http://www.cerezyme.com/home/default.asp")
             .addUrl("https://druginfo.nlm.nih.gov/drugportal/dpdirect.jsp?name=Cerezyme")
             .addUrl("http://www.nlm.nih.gov/medlineplus/druginfo/meds/a601149.html")
             .addUrl("https://drugs.ncats.io/drug/Q6U6J48BWY")
             .addIndication("Enzyme replacement therapy in patients with type I Gaucher's disease.")
             .addProperty("sponsor", "Genzyme Corporation"))
            .build();
        return object;
    }

    public static String toJsonString (Thing t) throws Exception {
        return toJsonString (t, true);
    }

    public static String toJsonString (Thing t, boolean pretty)
        throws Exception {
        JsonLdSerializer serializer = new JsonLdSerializer (pretty);
        return serializer.serialize(t);
    }

    public static List<Thing> fromJsonString (String json) throws Exception {
        JsonLdSerializer serializer = new JsonLdSerializer (true);
        return serializer.deserialize(json);
    }
}
