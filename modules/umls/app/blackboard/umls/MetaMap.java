package blackboard.umls;

import java.util.List;

import gov.nih.nlm.nls.metamap.AcronymsAbbrevs;
import gov.nih.nlm.nls.metamap.ConceptPair;
import gov.nih.nlm.nls.metamap.Ev;
import gov.nih.nlm.nls.metamap.MatchMap;
import gov.nih.nlm.nls.metamap.Mapping;
import gov.nih.nlm.nls.metamap.MetaMapApi;
import gov.nih.nlm.nls.metamap.MetaMapApiImpl;
import gov.nih.nlm.nls.metamap.Negation;
import gov.nih.nlm.nls.metamap.PCM;
import gov.nih.nlm.nls.metamap.Phrase;
import gov.nih.nlm.nls.metamap.Position;
import gov.nih.nlm.nls.metamap.Result;
import gov.nih.nlm.nls.metamap.Utterance;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import se.sics.prologbeans.PBTerm;
import play.libs.Json;

public class MetaMap {
    // Jackson can't serialize PBTerm, so we need to mask these and other
    // deprecated methods out via the mix-in class
    public abstract class MetaMapMixIn {
        @JsonIgnore abstract List<AcronymsAbbrevs> getAcronymsAbbrevs()
            throws Exception ;
        @JsonIgnore abstract List<Negation> getNegations() throws Exception;
        @JsonIgnore abstract List<gov.nih.nlm.nls.metamap.Map> getMappings()
            throws Exception ;
        @JsonIgnore abstract List<Ev> getCandidates() throws Exception;
        @JsonIgnore abstract PBTerm getMMOPBlist ();
        @JsonIgnore abstract PBTerm getTerm () throws Exception;
        @JsonIgnore abstract List<Object> getListRepr() throws Exception;
        @JsonIgnore abstract List<Object> getMatchMap() throws Exception;
        @JsonIgnore abstract PBTerm getMincoMan ();
        @JsonIgnore abstract String getMachineOutput ();
    }
    
    final MetaMapApi api;
    
    public MetaMap () {
        this (MetaMapApi.DEFAULT_SERVER_HOST,
              MetaMapApi.DEFAULT_SERVER_PORT,
              MetaMapApi.DEFAULT_TIMEOUT);
    }

    public MetaMap (String host, int port) {
        this (host, port, MetaMapApi.DEFAULT_TIMEOUT);
    }

    public MetaMap (int port) {
        this (MetaMapApi.DEFAULT_SERVER_HOST,
              port, MetaMapApi.DEFAULT_TIMEOUT);
    }

    public MetaMap (String host) {
        this (host, MetaMapApi.DEFAULT_SERVER_PORT,
              MetaMapApi.DEFAULT_TIMEOUT);
    }
    
    public MetaMap (String host, int port, int timeout) {
        api = new MetaMapApiImpl (host, port, timeout);
    }

    public List<Result> annotate (String text) throws Exception {
        return api.processCitationsFromString(text);
    }

    public JsonNode annotateAsJson (String text) throws Exception {
        List<Result> results = api.processCitationsFromString(text);
        return results.isEmpty() ? Json.newObject() : toJson (results.get(0));
    }

    public static JsonNode toJson (Object value) {
        ObjectMapper mapper = new ObjectMapper ();
        mapper.addMixInAnnotations(Result.class, MetaMapMixIn.class);
        mapper.addMixInAnnotations(PCM.class, MetaMapMixIn.class);
        mapper.addMixInAnnotations(Ev.class, MetaMapMixIn.class);
        mapper.addMixInAnnotations(MatchMap.class, MetaMapMixIn.class);
        mapper.addMixInAnnotations(Phrase.class, MetaMapMixIn.class);
        return mapper.valueToTree(value);
    }
}
