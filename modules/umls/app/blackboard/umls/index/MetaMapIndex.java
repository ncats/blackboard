package blackboard.umls.index;

import play.Logger;
import play.libs.Json;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

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

import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.util.BytesRef;

import blackboard.umls.MetaMap;
import blackboard.index.Index;

public class MetaMapIndex extends Index implements UMLSFields {
    public static class MMConcept extends Concept {
        public MMConcept (String ui, String name) {
            super (ui, name);
        }
        public MMConcept (String ui, String name, String type) {
            super (ui, name, type);
        }
        
        @Override
        public String[] getTokens () {
            if (context != null) {
                JsonNode json = (JsonNode)context;
                JsonNode nodes = json.get("matchedWords");
                if (nodes != null) {
                    String[] toks = new String[nodes.size()];
                    for (int i = 0; i < toks.length; ++i)
                        toks[i] = nodes.get(i).asText();
                    return toks;
                }
            }
            return super.getTokens();
        }
    }
    
    protected ObjectMapper mapper = new ObjectMapper ();    
    protected MetaMap metamap;

    protected MetaMapIndex (File dir) throws IOException {
        super (dir);
    }

    public void setMetaMap (MetaMap metamap) {
        this.metamap = metamap;
    }
    public MetaMap getMetaMap () { return metamap; }

    protected void instrument (Ev ev, Document doc) throws Exception {
        doc.add(new StringField (FIELD_CUI,
                                 ev.getConceptId(), Field.Store.NO));
        addTextField (doc, FIELD_CUI, ev.getConceptId());
        addTextField (doc, FIELD_CONCEPT, " cui=\""+ev.getConceptId()+"\"",
                      ev.getConceptName());
        doc.add(new FacetField (FACET_CUI, ev.getConceptId()));
        for (String t : ev.getSemanticTypes())
            doc.add(new FacetField (FACET_SEMTYPE, t));
        for (String s : ev.getSources())
            doc.add(new FacetField (FACET_SOURCE, s));
    }
    
    protected JsonNode metamap (Document doc, String text) {
        JsonNode json = null;
        if (metamap != null) {
            try {
                ArrayNode nodes = mapper.createArrayNode();
                for (Result r : metamap.annotate(text)) {
                    for (AcronymsAbbrevs abrv : r.getAcronymsAbbrevsList()) {
                        for (String cui : abrv.getCUIList()) {
                            doc.add(new StringField
                                    (FIELD_CUI, cui, Field.Store.NO));
                            addTextField (doc, FIELD_CUI, cui);
                        }
                    }
                    
                    for (Utterance utter : r.getUtteranceList()) {
                        for (PCM pcm : utter.getPCMList()) {
                            for (Mapping map : pcm.getMappingList())
                                for (Ev ev : map.getEvList()) {
                                    instrument (ev, doc);
                                }
                        }
                    }
                    json = MetaMap.toJson(r);
                    //Logger.debug(">>> "+json);
                    nodes.add(json);
                }
                
                json = nodes;
            }
            catch (Exception ex) {
                Logger.error("Can't annotate doc "+doc+" with MetaMap", ex);
            }
        }
        return json;
    }

    public static List<Concept> parseConcepts (JsonNode result) {
        //Logger.debug("MetaMap ===> "+result);
        JsonNode pcmList = result.at("/utteranceList/0/pcmlist");
        Set<Concept> concepts = new LinkedHashSet<>();
        for (int i = 0; i < pcmList.size(); ++i) {
            JsonNode pcm = pcmList.get(i);
            JsonNode evList = pcm.at("/mappingList/0/evList");
            //Logger.debug("evList ===> "+evList);
            if (!evList.isMissingNode()) {
                JsonNode text = pcm.at("/phrase/phraseText");
                for (int j = 0; j < evList.size(); ++j) {
                    JsonNode n = evList.get(j);
                    MMConcept c = new MMConcept
                        (n.get("conceptId").asText(),
                         n.get("preferredName").asText(),
                         null);
                    c.score = n.get("score").asInt();
                    concepts.add(c);
                
                    JsonNode types = n.get("semanticTypes");
                    for (int k = 0; k < types.size(); ++k)
                        c.types.add(types.get(k).asText());

                    ObjectNode ctx = Json.newObject();
                    ctx.put("phraseText", text.asText());
                    ctx.put("matchedWords", n.get("matchedWords"));
                    ctx.put("sources", n.get("sources"));
                    c.context = ctx;
                    concepts.add(c);
                }
            }
        }
        return new ArrayList<>(concepts);
    }
}
