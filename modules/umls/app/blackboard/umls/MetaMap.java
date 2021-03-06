package blackboard.umls;

import java.util.List;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;

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
import play.Logger;

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
        return api.processCitationsFromString(toAscii (text));
    }

    public JsonNode annotateAsJson (String text) throws Exception {
        List<Result> results = annotate (text);
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

    public static String toAscii (String text) {
        StringBuilder ascii = new StringBuilder ();
        CharsetEncoder enc = Charset.forName("ASCII").newEncoder();
        for (int i = 0, len = text.length(); i < len; ++i) {
            char ch = text.charAt(i);
            switch (ch) {
            case 'Ê': 
            case 'È':
            case 'É':
                ascii.append('E');
                break;
            case 'ë': 
            case 'é':
            case 'è':
            case 'ĕ':
            case 'ę':
                ascii.append('e');
                break;
            case 'ü':
            case 'ú':
            case 'μ':
                ascii.append('u');
                break;
            case 'Å':
            case 'Ä':
            case 'Â':
            case 'Ã':
                ascii.append('A');
                break;
            case 'å':
            case 'à':
            case 'á':
            case 'ä':
            case 'ã':
                ascii.append('a');
                break;
            case 'ß':
                ascii.append("ss");
                break;
            case 'ç': ascii.append('c'); break;
            case 'Ç': ascii.append('C'); break;
            case 'Ö':
            case 'Ó':
                ascii.append('O');
                break;
            case 'ñ': ascii.append('n'); break;
            case 'Ñ': ascii.append('N'); break;
            case 'Ü': ascii.append('U'); break;
            case 'ö':
            case 'ô':
            case 'ø':
            case 'ó':
            case 'ō':
                //case 'º':
                ascii.append('o');
                break;
            case '£': ascii.append('L'); break;
            case 'ł': ascii.append('l'); break;
            case '₅':
            case 'ş':
                ascii.append('s');
                break;
            case 'ĭ':
            case 'ï':
                ascii.append('i');
                break;
            case '°': ascii.append("{^o}"); break;
            case '∼': ascii.append('~'); break;
            case '≈': ascii.append("[approx]"); break;
            case 'α': ascii.append("[alpha]"); break;
            case 'β': ascii.append("[beta]"); break;
            case 'Δ': ascii.append("[DELTA]"); break;
            case 'δ': ascii.append("[delta]"); break;
            case 'ε': ascii.append("[epsilon]"); break;
            case 'γ': ascii.append("[gamma]"); break;
            case 'Κ': ascii.append("[KAPPA]"); break;
            case 'κ': ascii.append("[kappa]"); break;
            case 'λ': ascii.append("[lambda]"); break;
            case 'Μ': ascii.append("[MU]"); break;
            case 'Ω': ascii.append("[OMEGA]"); break;
            case 'ω': ascii.append("[omega]"); break;
            case 'ν': ascii.append("[nu]"); break;
            case 'φ': 
            case 'Φ':
                ascii.append("[phi]");
                break;
            case 'π': ascii.append("[pi]"); break;
            case 'ψ': ascii.append("[psi]"); break;
            case 'ρ': ascii.append("[rho]"); break;
            case '∑': 
            case 'Σ':
                ascii.append("[SIGMA]");
                break;
            case 'σ': ascii.append("[sigma]"); break;
            case 'τ': ascii.append("[tau]"); break;
            case '×': ascii.append('x'); break;
            case '±': ascii.append("+/-"); break;
            case '≧':
            case '≥':
                ascii.append(">=");
                break;
            case '≦':
            case '≤': 
            case '⩽':
                ascii.append("<=");
                break;
            case '♀': ascii.append("[maternal]"); break; // or [female]
            case '♂': ascii.append("[paternal]"); break; // or [male]
            case '₀': ascii.append("{_0}"); break;
            case '₁': ascii.append("{_1}"); break;
            case '¹': ascii.append("{^1}"); break;
            case '²': ascii.append("{^2}"); break;
            case '₂': ascii.append("{_2}"); break;
            case '³': ascii.append("{^3}"); break;
            case '₄': ascii.append("{_4}"); break;
            case '©': ascii.append("(c)"); break;
            case '®': ascii.append("(R)"); break;
            case '™': ascii.append("[tm]"); break;
            case '‰': ascii.append("[permil]"); break;
            case '℃': ascii.append("[celsius]"); break;
            case '€': ascii.append("[euro]"); break;
            case '•':
            case '·':
                ascii.append('*');
                break;
            case '´': ascii.append('\''); break;
            case '˙': ascii.append("{^*}"); break;
            case '–': ascii.append('-'); break;
            case '〈': ascii.append('<'); break;
            case '〉': ascii.append('>'); break;
            case ' ': ascii.append('_'); break;
            case ' ': ascii.append(' '); break;
            default:
                if (enc.canEncode(ch))
                    ascii.append(ch);
                else {
                    ascii.append("[.]");
                    Logger.warn("***** Unable to encode '"
                                +ch+"' to ASCII! ******");
                }
            }
        }
        //Logger.debug(">>"+text+"\n<<"+ascii+"\n");
        return ascii.toString();
    }
}
