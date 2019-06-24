package blackboard.pubmed;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.Calendar;
import static java.util.Calendar.*;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import blackboard.mesh.MeshDb;
import blackboard.mesh.Descriptor;
import blackboard.mesh.Qualifier;
import blackboard.mesh.Entry;

public class PubMedDoc implements java.io.Serializable {
    static private final Long serialVerionUID = 0x010101l;
    public static final PubMedDoc EMPTY = new PubMedDoc ();

    public static final String[] GRANT_ACTIVITY_CODE = {
        "C06", "D43", "D71", "DP1", "DP2", "DP3", "DP4", "DP5", "DP7",
        "E11", "F05", "F30", "F31", "F32", "F33", "F37", "F38", "F99",
        "FI2", "G07", "G08", "G11", "G12", "G13", "G20", "G94", "H13",
        "H25", "H50", "H57", "H62", "H64", "H75", "H79", "HD4", "I01",
        "I80", "IK3", "K00", "K01", "K02", "K05", "K06", "K07", "K08",
        "K12", "K14", "K18", "K21", "K22", "K23", "K24", "K25", "K26",
        "K30", "K38", "K43", "K76", "K99", "KD1", "KL1", "KL2", "KM1",
        "L30", "L32", "L40", "L50", "L60", "M01", "OT1", "OT2", "OT3",
        "P01", "P20", "P2C", "P30", "P40", "P41", "P42", "P50", "P51",
        "P60", "PL1", "PM1", "PN1", "PN2", "R00", "R01", "R03", "R13",
        "R15", "R18", "R21", "R24", "R25", "R28", "R30", "R33", "R34",
        "R35", "R36", "R37", "R38", "R41", "R42", "R43", "R44", "R49",
        "R50", "R55", "R56", "R61", "R90", "RC1", "RC2", "RC3", "RC4",
        "RF1", "RL1", "RL2", "RL5", "RL9", "RM1", "RS1", "S06", "S07",
        "S10", "S11", "S21", "S22", "SB1", "SC1", "SC2", "SC3", "SI2",
        "T01", "T02", "T09", "T14", "T15", "T32", "T34", "T35", "T37",
        "T42", "T90", "TL1", "TL4", "TU2", "U01", "U09", "U10", "U11",
        "U13", "U17", "U18", "U19", "U1A", "U1B", "U1Q", "U1V", "U21",
        "U22", "U23", "U24", "U27", "U2C", "U2G", "U2R", "U30", "U32",
        "U34", "U36", "U38", "U41", "U42", "U43", "U44", "U45", "U47",
        "U48", "U49", "U50", "U51", "U52", "U53", "U54", "U55", "U56",
        "U57", "U58", "U59", "U60", "U61", "U62", "U65", "U66", "U75",
        "U79", "U81", "U82", "U83", "U84", "U90", "UA1", "UA5", "UB1",
        "UC1", "UC2", "UC3", "UC4", "UC6", "UC7", "UD1", "UE1", "UE2",
        "UE5", "UF1", "UF2", "UG1", "UG3", "UG4", "UH1", "UH2", "UH3",
        "UH4", "UL1", "UM1", "UM2", "UP5", "UR6", "UR8", "US3", "US4",
        "UT1", "UT2", "VF1", "VF1", "X01", "X02", "X98", "X99"
    };
    
    public static class Author {
        public final String lastname;
        public final String forename;
        public final String initials;
        public final String collectiveName;
        public final String[] affiliations;
        public final String identifier;

        Author (Element elm) {
            NodeList nodes = elm.getElementsByTagName("LastName");
            if (nodes.getLength() == 0) {
                nodes = elm.getElementsByTagName("CollectiveName");
                if (nodes.getLength() == 0)
                    throw new IllegalArgumentException
                        ("Author has no valid name!");
                else
                    collectiveName = getText (nodes.item(0));
            }
            else
                collectiveName = null;
            lastname = getText (nodes.item(0));
            nodes = elm.getElementsByTagName("ForeName");
            forename = nodes == null || nodes.getLength() == 0
                ? null : getText (nodes.item(0));
            nodes = elm.getElementsByTagName("Initials");
            initials = nodes == null || nodes.getLength() == 0
                ? null : getText (nodes.item(0));
            
            nodes = elm.getElementsByTagName("Affiliation");
            affiliations = new String[nodes.getLength()];
            for (int i = 0; i < nodes.getLength(); ++i) {
                affiliations[i] = getText (nodes.item(i));
            }

            nodes = elm.getElementsByTagName("Identifier");
            identifier = nodes == null || nodes.getLength() == 0
                ? null : getText (nodes.item(0));
        }

        Author (Map<String, Object> auth) {
            lastname = (String) auth.get("LastName");
            collectiveName = (String) auth.get("CollectiveName");
            if (lastname == null && collectiveName == null)
                throw new IllegalArgumentException
                    ("Author element doesn't have LastName!");
            forename = (String) auth.get("ForeName");
            initials = (String) auth.get("Initials");
            affiliations = (String[]) auth.get("Affiliation");
            identifier = (String) auth.get("Identifier");
        }

        public String getName () {
            if (collectiveName != null)
                return collectiveName;
            if (forename == null && initials == null)
                return lastname;
            if (forename == null && initials != null)
                return lastname+", "+initials;
            return lastname+", "+forename;
        }
    }

    public static class Grant {
        public final String type;
        public final String id;
        public final String acronym;
        public final String agency;
        public final String country;

        Grant (Map<String, Object> grant) {
            String grantId = (String) grant.get("GrantID");
            String[] tuple = parseGrantId (grantId);
            type = tuple[0];
            id = tuple[1];
            this.acronym = (String) grant.get("Acronym");
            this.agency = (String) grant.get("Agency");
            this.country = (String) grant.get("Country");
        }

        Grant (Element elm) {
            NodeList nodes = elm.getElementsByTagName("GraphID");
            String grantId = nodes.getLength() > 0
                ? getText (nodes.item(0)) : null;
            String[] tuple = parseGrantId (grantId);
            type = tuple[0];
            id = tuple[1];
            nodes = elm.getElementsByTagName("Acronym");
            acronym = nodes.getLength() > 0 ? getText (nodes.item(0)) : null;
            nodes = elm.getElementsByTagName("Agency");
            agency = nodes.getLength() > 0 ? getText (nodes.item(0)) : null;
            nodes = elm.getElementsByTagName("Country");
            country = nodes.getLength() > 0 ? getText (nodes.item(0)) : null;
        }
        
        String[] parseGrantId (String grantId) {
            String t = null, i = null;
            if (grantId != null) {
                for (String act : GRANT_ACTIVITY_CODE) {
                    if (grantId.startsWith(act)) {
                        t = act;
                        i = grantId.substring(act.length()).trim();
                        if (i.charAt(0) == '-')
                            i = i.substring(1);
                        break;
                    }
                }
                
                if (i == null)
                    i = grantId;
            }
            return new String[]{t, i};
        }
    }

    public static class Reference {
        public final String citation;
        public final Long[] pmids;

        Reference (Element elm) {
            NodeList nodes = elm.getElementsByTagName("Citation");
            if (nodes == null || nodes.getLength() == 0)
                throw new IllegalArgumentException
                    ("Reference elmenent has not citation!");
            citation = getText (nodes.item(0));
            nodes = elm.getElementsByTagName("ArticleId");
            List<Long> refs = new ArrayList<>();
            for (int i = 0; i < nodes.getLength(); ++i) {
                Element e = (Element)nodes.item(i);
                if ("pubmed".equals(e.getAttribute("IdType"))) {
                    try {
                        refs.add(Long.parseLong(getText(e)));
                    }
                    catch (NumberFormatException ex) {
                    }
                }
            }
            pmids = refs.toArray(new Long[0]);
        }
        Reference (Map<String, Object> ref) {
            citation = (String)ref.get("Citation");
            pmids = (Long[])ref.get("pubmed");
        }
    }
        
    public Long pmid;
    public String doi;
    public String pmc;
    public String title;
    public List<String> abs = new ArrayList<>();
    public List<Author> authors = new ArrayList<>();
    public List<String> keywords = new ArrayList<>();
    public List<Grant> grants = new ArrayList<>();
    public String journal;
    public Date date;
    public Date revised;
    public String lang; // other language for abstract
    public List<Entry> pubtypes = new ArrayList<>();
    public List<MeshHeading> headings = new ArrayList<>();
    public List<Entry> chemicals = new ArrayList<>();
    public List<Reference> references = new ArrayList<>();
    public List<Author> investigators = new ArrayList<>();

    public byte[] xml; // raw xml
    public String source; // input source for xml
    public Long timestamp;

    static String getText (Node node) {
        if (node instanceof Element)
            return ((Element)node).getTextContent();
        return null;
    }
    
    protected PubMedDoc () {
    }

    public static PubMedDoc getInstance (Document doc, MeshDb mesh) {
        return new PubMedDoc (doc, mesh);
    }
    
    protected PubMedDoc (Document doc, MeshDb mesh) {
        NodeList nodes = doc.getElementsByTagName("PMID");
        pmid = nodes.getLength() > 0
            ? Long.parseLong(((Element)nodes.item(0)).getTextContent()) : null;
        nodes = doc.getElementsByTagName("ArticleTitle");
        title = nodes.getLength() > 0
            ? ((Element)nodes.item(0)).getTextContent() : null;
        nodes = doc.getElementsByTagName("Abstract");
        if (nodes.getLength() > 0) {
            Element elm = (Element)nodes.item(0);
            nodes = elm.getElementsByTagName("AbstractText");
            if (nodes.getLength() > 0) {
                for (int i = 0; i < nodes.getLength(); ++i) {
                    elm = (Element)nodes.item(i);
                    String cat = elm.getAttribute("Label");
                    if (cat != null && !"".equals(cat))
                        cat = cat + ": ";
                    else
                        cat = "";
                    abs.add(cat+elm.getTextContent());
                }
            }
            else {
                abs.add(elm.getTextContent());
            }
        }

        nodes = doc.getElementsByTagName("OtherAbstract");
        if (nodes.getLength() > 0) {
            Element elm = (Element)nodes.item(0);
            lang = elm.getAttribute("Language");
        }

        nodes = doc.getElementsByTagName("DateRevised");
        if (nodes.getLength() > 0) {
            revised = parseDate (nodes.item(0));
        }
        
        /*
         * journal 
         */
        nodes = doc.getElementsByTagName("Journal");
        if (nodes.getLength() > 0) {
            Element elm = (Element)nodes.item(0);
            nodes = elm.getElementsByTagName("Title");
            journal = nodes.getLength() > 0
                ? ((Element)nodes.item(0)).getTextContent() : null;
            nodes = elm.getElementsByTagName("PubDate");
            if (nodes.getLength() > 0) {
                date = parseDate (nodes.item(0));
            }
        }

        /*
         * author
         */
        nodes = doc.getElementsByTagName("Author");
        for (int i = 0; i < nodes.getLength(); ++i) {
            try {
                Author auth = new Author ((Element)nodes.item(i));
                authors.add(auth);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        /*
         * keywords
         */
        nodes = doc.getElementsByTagName("Keyword");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            keywords.add(elm.getTextContent());
        }
        
        /*
         * grant
         */
        nodes = doc.getElementsByTagName("Grant");
        for (int i = 0; i < nodes.getLength(); ++i) {
            try {
                Grant g = new Grant ((Element)nodes.item(i));
                grants.add(g);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        /*
         * publication type
         */
        nodes = doc.getElementsByTagName("PublicationType");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            String ui = elm.getAttribute("UI");
            Entry desc = mesh.getEntry(ui);
            if (desc != null)
                pubtypes.add(desc);
        }

        /*
         * references
         */
        nodes = doc.getElementsByTagName("Reference");
        for (int i = 0; i < nodes.getLength(); ++i) {
            try {
                Reference ref = new Reference ((Element)nodes.item(i));
                references.add(ref);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        /*
         * other identifiers
         */
        nodes = doc.getElementsByTagName("ArticleId");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            String type = elm.getAttribute("IdType");
            if ("doi".equals(type)) {
                doi = elm.getTextContent();
            }
            else if ("pmc".equals(type)) {
                pmc = elm.getTextContent();
            }
        }

        nodes = doc.getElementsByTagName("MeshHeading");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            NodeList nl = elm.getElementsByTagName("DescriptorName");
            MeshHeading mh = null;
            if (nl.getLength() > 0) {
                String ui = ((Element)nl.item(0)).getAttribute("UI");
                String major =
                    ((Element)nl.item(0)).getAttribute("MajorTopicYN");
                Entry desc = mesh.getEntry(ui);
                if (desc != null) {
                    mh = new MeshHeading (desc, "Y".equals(major));
                    headings.add(mh);
                }
            }

            if (mh != null) {
                nl = elm.getElementsByTagName("QualifierName");
                for (int j = 0; j < nl.getLength(); ++j) {
                    elm = (Element)nl.item(j);
                    String ui = elm.getAttribute("UI");
                    Entry qual = mesh.getEntry(ui);
                    if (qual != null)
                        mh.qualifiers.add(qual);
                }
            }
        }

        nodes = doc.getElementsByTagName("NameOfSubstance");
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element elm = (Element)nodes.item(i);
            String ui = elm.getAttribute("UI");
            Entry chem = mesh.getEntry(ui);
            if (chem != null)
                chemicals.add(chem);
        }

        /*
         * investigator
         */
        nodes = doc.getElementsByTagName("Investigator");
        for (int i = 0; i < nodes.getLength(); ++i) {
            try {
                Author auth = new Author ((Element)nodes.item(i));
                investigators.add(auth);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    static Date parseDate (Node node) {
        Date date = null;
        if (node instanceof Element) {
            Element elm = (Element) node;
            
            Calendar cal = Calendar.getInstance();
            NodeList nodes = elm.getElementsByTagName("Year");
            if (nodes.getLength() > 0) {
                cal.set(YEAR, Integer.parseInt
                        (((Element)nodes.item(0)).getTextContent()));
            }
            
            nodes = elm.getElementsByTagName("Month");
            int month = 0;
            if (nodes.getLength() > 0) {
                String mon = ((Element)nodes.item(0)).getTextContent();
                month = parseMonth (mon);
            }
            cal.set(MONTH, month);
            
            nodes = elm.getElementsByTagName("Day");
            if (nodes.getLength() > 0) {
                    cal.set(DAY_OF_MONTH, Integer.parseInt
                            (((Element)nodes.item(0)).getTextContent()));
            }
            else {
                cal.set(DAY_OF_MONTH, 1);
            }
            
            date = cal.getTime();
        }
        return date;
    }

    public Long getPMID () { return pmid; }
    public String getTitle () { return title; }
    public List<String> getAbstract () { return abs; }
    public Date getDate () { return date; }
    public int getYear () {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.YEAR);
    }
    public String getDOI () { return doi; }
    public String getPMC () { return pmc; }
    public String getJournal () { return journal; }
    public List<MeshHeading> getMeshHeadings () { return headings; }
    public List<Entry> getChemicals () { return chemicals; }

    public PubMedDoc addAuthor (Map<String, Object> author) {
        authors.add(new Author (author));
        return this;
    }
    public PubMedDoc addInvestigator (Map<String, Object> author) {
        investigators.add(new Author (author));
        return this;
    }
    public PubMedDoc addGrant (Map<String, Object> grant) {
        grants.add(new Grant (grant));
        return this;
    }
    public PubMedDoc addReference (Map<String, Object> reference) {
        references.add(new Reference (reference));
        return this;
    }

    public static int parseMonth (String mon) {
        int month = 0;
        switch (mon) {
        case "jan": case "Jan":
            month = JANUARY;
            break;
        case "feb": case "Feb":
            month = FEBRUARY;
            break;
        case "mar": case "Mar":
            month = MARCH;
            break;
        case "apr": case "Apr":
            month = APRIL;
            break;
        case "may": case "May":
            month = MAY;
            break;
        case "jun": case "Jun":
            month = JUNE;
            break;
        case "jul": case "Jul":
            month = JULY;
            break;
        case "aug": case "Aug":
            month = AUGUST;
            break;
        case "sep": case "Sep":
            month = SEPTEMBER;
            break;
        case "oct": case "Oct":
            month = OCTOBER;
            break;
        case "nov": case "Nov":
            month = NOVEMBER;
            break;
        case "dec": case "Dec":
            month = DECEMBER;
            break;
        default:
            try {
                int m = Integer.parseInt(mon);
                month = m - 1; // 0-based
            }
            catch (NumberFormatException ex) {
                throw new RuntimeException ("Unknown month: "+mon);
            }
        }
        return month;
    }
}
