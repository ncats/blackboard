package blackboard.pubmed.index;

import play.Logger;
import play.libs.Json;

import blackboard.pubmed.*;
import blackboard.mesh.MeshDb;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.Descriptor;
import blackboard.index.MetaMapIndex;
import blackboard.semmed.SemMedDbKSource;
import blackboard.semmed.Predication;
import blackboard.umls.SemanticType;
import blackboard.umls.UMLSKSource;
import blackboard.umls.Concept;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.temporal.ChronoField;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.lucene.store.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.suggest.document.*;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.lang3.StringUtils;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.DocumentType;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import com.google.inject.assistedinject.Assisted;

public class PubMedIndex extends MetaMapIndex {
    // MetaMap compressed json
    public static final String FIELD_MM_TITLE = "mm_title";
    public static final String FIELD_MM_ABSTRACT = "mm_abstract";
    public static final int MAX_FACET_FIELD_LENGTH = 1024;

    public static final Map<String, Object> EMPTY_FACETS = new HashMap<>();
    public static final MatchedDoc EMPTY_DOC = new MatchedDoc ();
    public static final SearchResult EMPTY_RESULT = new SearchResult ();

    public static class MatchedFragment {
        public final String fragment;
        public final String field;
        public final String text;

        MatchedFragment (String text) {
            String f = null, t = null;
            // locate <b>...</b>
            int pos = text.indexOf("<b>");
            if (pos >= 0) {
                int bgn = pos;
                while (--bgn > 0 && text.charAt(bgn) != '>')
                    ;
                for (int i = bgn; --i >= 0; ) {
                    if (text.startsWith("fn=\"", i)) {
                        i += 4;
                        int j = i;
                        for (; j < bgn && text.charAt(j) != '"'; ++j)
                            ;
                        f = text.substring(i, j);
                        break;
                    }
                }
                
                pos = text.indexOf("</b>", pos);
                if (pos > 0) {
                    pos += 4;
                    for (int i = pos; i < text.length(); ++i) {
                        if (text.startsWith("</fld", i)) {
                            if (text.charAt(bgn) == '>')
                                ++bgn;
                            t = text.substring(bgn, i);
                            break;
                        }
                    }
                    
                    if (t == null) {
                        if (text.charAt(bgn) == '>')
                            ++bgn;
                        t = text.substring(bgn);
                    }
                }
            }
            this.field = f;
            this.fragment = t;
            this.text = text;
        }

        public String toString () {
            return "MatchedFragment{text="+text+",field="+field
                +",fragment="+fragment+"}";
        }
    }
    
    public static class MatchedDoc {
        public Float score;
        public Long pmid;
        public String title;
        public Integer year;
        public List<MatchedFragment> fragments = new ArrayList<>();
        public Map<String, String> concepts = new TreeMap<>();

        @JsonIgnore
        public org.w3c.dom.Document doc;

        protected MatchedDoc () {
        }
        
        protected MatchedDoc (Long pmid, String title, Integer year) {
            this.pmid = pmid;
            this.title = title;
            this.year = year;
        }

        public String toXmlString () {
            if (doc != null) {
                try {
                    StringWriter buf = new StringWriter (1024);
                    TransformerFactory.newInstance().newTransformer()
                        .transform(new DOMSource (doc), new StreamResult (buf));
                    return buf.toString();
                }
                catch (Exception ex) {
                    Logger.error("Can't write xml string", ex);
                }
            }
            return null;
        }
    }

    public static class SearchResult
        extends blackboard.index.Index.SearchResult {
        public final List<MatchedDoc> docs = new ArrayList<>();
        public final Map<Integer, Integer> years = new TreeMap<>();
        public final Map<String, String> concepts = new TreeMap<>();
        
        final MeshDb mesh;
        final UMLSKSource umls;

        protected SearchResult () {
            this (null, null);
        }
            
        protected SearchResult (MeshKSource mesh, UMLSKSource umls) {
            this.mesh = mesh != null ? mesh.getMeshDb() : null;
            this.umls = umls;
        }
        
        @Override
        public int size () { return docs.size(); }
        
        protected boolean process (blackboard.index.Index.ResultDoc rdoc) {
            try {
                MatchedDoc mdoc = toMatchedDoc (rdoc.doc);
                String[] frags = rdoc.getFragments(FIELD_TEXT, 500, 10);
                if (frags != null) {
                    for (String f : frags)
                        mdoc.fragments.add(new MatchedFragment (f));
                }
                
                if (mdoc.year != null) {
                    Integer c = years.get(mdoc.year);
                    years.put(mdoc.year, c!=null ? c+1:1);
                }

                concepts.putAll(mdoc.concepts);
                mdoc.score = rdoc.score;
                docs.add(mdoc);
            }
            catch (IOException ex) {
                Logger.error("Can't process doc "+rdoc.docId, ex);
            }
            return true;
        }

        protected void updateFacets () {
            for (Facet f : facets) {
                switch (f.name) {
                case FIELD_TR: // treeNumber
                    if (mesh != null) {
                        for (FV fv : f.values)
                            updateTreeNumberDisplay (fv);
                    }
                    break;

                case FIELD_UI:
                case FIELD_PUBTYPE:
                    if (mesh != null) {
                        for (FV fv : f.values) {
                            Descriptor desc =
                                (Descriptor)mesh.getEntry(fv.label);
                            //Logger.debug(fv.label +" => "+desc);
                            if (desc != null)
                                fv.display = desc.getName();
                        }
                    }
                    break;

                case FIELD_SEMTYPE:
                    if (umls != null) {
                        for (FV fv : f.values) {
                            SemanticType st = umls.getSemanticType(fv.label);
                            if (st != null) {
                                fv.display = st.name;
                            }
                            else {
                                Logger.warn("Unknown semantic type: "+fv.label);
                            }
                        }
                    }
                    break;

                case FIELD_CUI:
                    for (FV fv : f.values) {
                        /*
                        try {
                            Concept concept = umls.getConcept(fv.label);
                            if (concept != null)
                                fv.display = concept.name;
                        }
                        catch (Exception ex) {
                            Logger.error("Can't retrieve UMLS concept: "
                                         +fv.label, ex);
                        }
                        */
                        fv.display = concepts.get(fv.label);
                    }
                    break;
                }
            }
        }

        protected void updateTreeNumberDisplay (FV fv) {
            if (fv .display == null) {
                String path = StringUtils.join(fv.toPath(), '.');
                List<Descriptor> desc =
                    mesh.getDescriptorsByTreeNumber(path);
                if (desc.isEmpty())
                    Logger.warn
                        ("No Descriptor found for tree nubmer: "+path);
                else {
                    if (desc.size() > 1) {
                        Logger.warn
                            (desc.size()
                             +" descriptors found for tree nubmer: "
                             +path);
                    }
                    fv.display = desc.get(0).getName();
                }
            }
            
            for (FV child : fv.children)
                updateTreeNumberDisplay (child);
        }

        public void exportXML (OutputStream os) throws Exception {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();
            org.w3c.dom.Document doc = builder.newDocument();
            //doc.setXmlStandalone(true);
            DocumentType doctype =
                doc.getImplementation().createDocumentType
                ("PubmedArticleSet",
                 "-//NLM//DTD PubMedArticle, 1st January 2019//EN",
                 "https://dtd.nlm.nih.gov/ncbi/pubmed/out/pubmed_190101.dtd");
            doc.appendChild(doctype);
            
            org.w3c.dom.Element container =
                doc.createElement("PubmedArticleSet");
            for (MatchedDoc md : docs) {
                if (md.doc != null) {
                    org.w3c.dom.Node node = doc.importNode
                        (md.doc.getDocumentElement(), true);
                    container.appendChild(node);
                }
                else {
                    Logger.warn(md.pmid+": No XML doc");
                }
            }
            TransformerFactory.newInstance().newTransformer()
                .transform(new DOMSource (container), new StreamResult (os));
        }
    }

    public static SearchResult merge (SearchResult... results) {
        return merge (0, 0, results);
    }
    
    public static SearchResult merge
        (int skip, int top, SearchResult... results) {
        
        SearchResult merged = new SearchResult ();
        Map<String, List<Facet>> facets = new TreeMap<>();
        List<MatchedDoc> docs = new ArrayList<>();
        for (SearchResult r : results) {
            docs.addAll(r.docs);
            merged.concepts.putAll(r.concepts);
            for (Map.Entry<Integer, Integer> me : r.years.entrySet()) {
                Integer c = merged.years.get(me.getKey());
                if (c == null) c = 0;
                merged.years.put(me.getKey(), c + me.getValue());
            }
            merged.total += r.total;

            for (Facet f : r.facets) {
                /*
                Logger.debug("#### "+f.toString());
                Facet c = clone (f);
                Logger.debug("**** "+c.toString());
                */
                List<Facet> fs = facets.get(f.name);
                if (fs == null)
                    facets.put(f.name, fs = new ArrayList<>());
                fs.add(f);
            }
        }

        for (List<Facet> fs : facets.values()) {
            Facet f = merge (fs.toArray(new Facet[0]));
            merged.facets.add(f);
        }

        /*
        Logger.debug("$$$$$$ merged facets...");
        for (Facet f : merged.facets)
            Logger.debug(f.toString());
        */
        
        Collections.sort(docs, (a, b) -> {
                float score = b.score - a.score;
                if (score > 0) return 1;
                else if (score < 0) return -1;
                int d = b.year - a.year;
                if (d == 0) {
                    if (b.pmid > a.pmid) d = 1;
                    else if (b.pmid < a.pmid) d = -1;
                    else d = a.title.compareTo(b.title);
                }
                return d;
            });

        if (top <= 0) {
            merged.docs.addAll(docs);
        }
        else {
            int max = Math.min(docs.size(), skip+top);
            merged.docs.addAll(docs.subList(skip, max));
        }
        return merged;
    }

    @Inject public SemMedDbKSource semmed;
    @Inject public MeshKSource mesh;
    @Inject public UMLSKSource umls;
    
    @Inject
    public PubMedIndex (@Assisted File dir) throws IOException {
        super (dir);
    }

    @Override
    protected FacetsConfig configFacets () {
        FacetsConfig fc = new FacetsConfig ();
        fc.setMultiValued(FIELD_TR, true);
        fc.setHierarchical(FIELD_TR, true);
        fc.setMultiValued(FIELD_UI, true);
        fc.setMultiValued(FIELD_SEMTYPE, true); // umls semantic types
        fc.setMultiValued(FIELD_SOURCE, true); // umls sources
        fc.setMultiValued(FIELD_CUI, true);
        fc.setMultiValued(FIELD_PREDICATE, true);
        fc.setMultiValued(FIELD_AUTHOR, true);
        fc.setMultiValued(FIELD_PUBTYPE, true);
        fc.setMultiValued(FIELD_JOURNAL, true);
        fc.setMultiValued(FIELD_KEYWORD, true);
        fc.setMultiValued(FIELD_REFERENCE, true);
        fc.setMultiValued(FIELD_GRANTTYPE, true);
        fc.setMultiValued(FIELD_GRANTID, true);
        fc.setHierarchical(FIELD_GRANTAGENCY, true);
        fc.setMultiValued(FIELD_GRANTAGENCY, true);
        fc.setMultiValued(FIELD_GRANTCOUNTRY, true);
        return fc;
    }

    public boolean indexed (Long pmid) throws IOException {
        try (IndexReader reader = DirectoryReader.open(indexWriter, true)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange
                (FIELD_PMID, pmid, pmid, true, true);
            TopDocs hits = searcher.search(query, 1);
            return hits.totalHits > 0;
        }
    }
    
    public boolean addIfAbsent (PubMedDoc d) throws IOException {
        if (!indexed (d.getPMID())) {
            add (d);
            return true;
        }
        return false;
    }

    protected Document instrument (Document doc, PubMedDoc d)
        throws IOException {
        doc.add(new LongField (FIELD_PMID, d.getPMID(), Field.Store.YES));
        addTextField (doc, FIELD_PMID, d.getPMID());
        if (d.timestamp != null)
            doc.add(new LongField (FIELD_TIMESTAMP,
                                   d.timestamp, Field.Store.YES));
        if (d.source != null)
            doc.add(new FacetField (FIELD_FILE, d.source));
        
        String title = d.getTitle();
        if (title != null && !"".equals(title)) {
            addTextField (doc, FIELD_TITLE, title);
            doc.add(new Field (FIELD_TITLE, title, tvFieldType));
        }

        // author
        for (PubMedDoc.Author auth : d.authors) {
            if (auth.affiliations != null) {
                for (String affi : auth.affiliations) {
                    /*
                    if (affi.length() > MAX_FACET_FIELD_LENGTH) {
                        Logger.warn(d.getPMID()+": Affiliation is too long (>"
                                    +MAX_FACET_FIELD_LENGTH+"); "
                                    +"truncating...\n"+affi);
                        affi = affi.substring(0, MAX_FACET_FIELD_LENGTH);
                    }
                    
                    if (affi.length() > 2)
                        doc.add(new FacetField (FIELD_AFFILIATION, affi));
                    */
                    addTextField (doc, FIELD_AFFILIATION, affi);
                    doc.add(new Field (FIELD_AFFILIATION, title, tvFieldType));
                }
            }
            doc.add(new FacetField (FIELD_AUTHOR, auth.getName()));
            addTextField (doc, FIELD_AUTHOR, auth.getName());
        }

        // journal
        if (d.journal != null && d.journal.length() > 0)
            doc.add(new FacetField (FIELD_JOURNAL, d.journal));

        // grants
        for (PubMedDoc.Grant grant : d.grants) {
            if (grant.type != null && grant.type.length() > 0)
                doc.add(new FacetField (FIELD_GRANTTYPE, grant.type));

            if (grant.id != null) {
                String[] toks = grant.id.split(",");
                for (String t : toks) {
                    String id = t.trim();
                    if (id.length() > 0) {
                        doc.add(new FacetField (FIELD_GRANTID, id));
                        addTextField (doc, FIELD_GRANTID, id);
                    }
                }
            }
            
            if (grant.agency != null && grant.agency.length() > 0) {
                if (grant.agency.indexOf('|') > 0) {
                    String[] toks = grant.agency.split("\\|");
                    for (int i = 0; i < toks.length; ++i)
                        toks[i] = toks[i].trim();
                    doc.add(new FacetField (FIELD_GRANTAGENCY, toks));
                }
                else if (grant.agency.endsWith("HHS")) {
                    String[] toks = grant.agency.split("[\\s]+");
                    for (int i = 0, j = toks.length-1; i < j; ++i, --j) {
                        String t = toks[i];
                        toks[i] = toks[j];
                        toks[j] = t;
                    }
                    if (toks.length > 0)
                        doc.add(new FacetField (FIELD_GRANTAGENCY, toks));
                }
                else
                    doc.add(new FacetField (FIELD_GRANTAGENCY, grant.agency));
            }
            
            if (grant.country != null && grant.country.length() > 0)
                doc.add(new FacetField (FIELD_GRANTCOUNTRY, grant.country));
        }

        // publication types
        for (blackboard.mesh.Entry e : d.pubtypes)
            doc.add(new FacetField (FIELD_PUBTYPE, e.ui));

        // keywords
        for (String k : d.keywords) {
            String keyword = WordUtils.capitalize(k);
            doc.add(new FacetField (FIELD_KEYWORD, keyword));
            addTextField (doc, FIELD_KEYWORD, k);
        }

        // abstract texts
        for (String abs : d.getAbstract())
            addTextField (doc, FIELD_ABSTRACT, abs);

        // publication year
        doc.add(new IntField (FIELD_YEAR, d.getYear(), Field.Store.YES));

        // mesh headings
        for (MeshHeading mh : d.getMeshHeadings()) {
            Descriptor desc = (Descriptor)mh.descriptor;
            doc.add(new FacetField (FIELD_UI, desc.ui));
            addTextField (doc, FIELD_UI, desc.ui);
            for (String tr : desc.treeNumbers) {
                Logger.debug("..."+tr);
                doc.add(new FacetField (FIELD_TR, tr.split("\\.")));
            }
            /*
            doc.add(new ContextSuggestField (FIELD_MESH, desc.name,
                                             FIELD_MESH_WEIGHT, desc.ui));
            */
            addTextField (doc, FIELD_MESH, " ui=\""+desc.ui+"\"", desc.name);
        }

        // identifiers
        if (d.pmc != null) {
            doc.add(new StringField (FIELD_PMC, d.pmc, Field.Store.YES));
            addTextField (doc, FIELD_PMC, d.pmc);
        }
        if (d.doi != null) {
            doc.add(new StringField (FIELD_DOI, d.doi, Field.Store.YES));
        }

        // references
        for (PubMedDoc.Reference ref : d.references) {
            if (ref.pmids != null)
                for (Long id : ref.pmids)
                    doc.add(new FacetField
                            (FIELD_REFERENCE, String.valueOf(id)));
        }
        
        /*
         * now do metamap
         */
        /*
        JsonNode json = metamap (doc, title);
        if (json != null && json.size() > 0) {
            BytesRef ref = new BytesRef (toCompressedBytes (json));
            doc.add(new StoredField (FIELD_MM_TITLE, ref));
        }
        
        for (String abs : d.getAbstract()) {
            json = metamap (doc, abs);
            if (json != null && json.size() > 0) {
                doc.add(new StoredField
                        (FIELD_MM_ABSTRACT,
                         new BytesRef (toCompressedBytes (json))));
            }
        }
        */

        /*
         * SemMedDb
         */
        if (semmed != null) {
            try {
                List<Predication> preds = semmed.getPredicationsByPMID
                    (String.valueOf(d.getPMID()));
                Map<String, String> cuis = new LinkedHashMap<>();
                Set<String> types = new LinkedHashSet<>();
                Set<String> predicates = new LinkedHashSet<>();
                for (Predication p : preds) {
                    cuis.put(p.subcui, p.subject);
                    cuis.put(p.objcui, p.object);
                    types.add(p.subtype);
                    types.add(p.objtype);
                    predicates.add(p.predicate);
                }

                for (Map.Entry<String, String> me : cuis.entrySet()) {
                    doc.add(new FacetField (FIELD_CUI, me.getKey()));
                    /*
                    doc.add(new ContextSuggestField
                            (FIELD_CONCEPT, me.getValue(),
                             FIELD_CONCEPT_WEIGHT, me.getKey()));
                    */
                    // store the concept as CUI:NAME so that we can replace
                    // the cui with its concept name in facet
                    doc.add(new StoredField
                            (FIELD_CONCEPT, me.getKey()+":"+me.getValue()));
                    
                    addTextField (doc, FIELD_CUI, me.getKey());
                    addTextField (doc, FIELD_CONCEPT, " cui=\""
                                  +me.getKey()+"\"", me.getValue());
                }
                for (String type : types)
                    doc.add(new FacetField (FIELD_SEMTYPE, type));
                for (String pred : predicates)
                    doc.add(new FacetField (FIELD_PREDICATE, pred));
            }
            catch (Exception ex) {
                Logger.error("Can't retrieve Predications for "
                             +d.getPMID(), ex);
            }
        }

        if (d.xml != null) {
            BytesRef ref = new BytesRef (toCompressedBytes (d.xml));
            doc.add(new StoredField (FIELD_XML, ref));
        }

        //Logger.debug("indexed... "+doc);
        return doc;
    }

    protected void add (Document doc) throws IOException {
        indexWriter.addDocument(facetConfig.build(taxonWriter, doc));
    }

    protected Document newDocument () {
        Document doc = new Document ();
        doc.add(new StringField
                (FIELD_INDEXER, getClass().getName(), Field.Store.YES));
        return doc;
    }
    
    public PubMedIndex add (PubMedDoc d) throws IOException {
        Logger.debug(Thread.currentThread().getName()+": "
                     +d.getPMID()+": "+d.getTitle());
        add (instrument (newDocument (), d));
        return this;
    }

    protected void debug () throws IOException {
        IndexSearcher searcher = null;        
        try {
            searcher = new IndexSearcher
                (DirectoryReader.open(indexWriter, true));
            TaxonomyReader taxonReader =
                new DirectoryTaxonomyReader (taxonWriter);
            FacetsCollector fc = new FacetsCollector ();
            searcher.search(new MatchAllDocsQuery (), fc);
            Facets facets = new FastTaxonomyFacetCounts
                (taxonReader, facetConfig, fc);
            /*
            Logger.debug(facets.getTopChildren
                         (20, "tr", "D02.455.426.559.847.638".split("\\."))
                         .toString());
            */
            FacetResult fr = facets.getTopChildren(20, FIELD_UI);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_CUI);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_SEMTYPE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_SOURCE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_AUTHOR);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_AFFILIATION);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_REFERENCE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FIELD_KEYWORD);
            if (fr != null)
                Logger.debug(fr.toString());

            IOUtils.close(taxonReader);
        }
        finally {
            if (searcher != null)
                IOUtils.close(searcher.getIndexReader());
        }
    }

    SearchResult createSearchResult () {
        return new SearchResult (mesh, umls);
    }

    static MatchedDoc toMatchedDoc (Document doc) throws IOException {
        return toMatchedDoc (new MatchedDoc (), doc);
    }
    
    static MatchedDoc toMatchedDoc (MatchedDoc md, Document doc)
        throws IOException {
        IndexableField field = doc.getField(FIELD_PMID);
        if (field != null) {
            md.pmid = field.numericValue().longValue();
            md.title = doc.get(FIELD_TITLE);
            md.year = doc.getField(FIELD_YEAR).numericValue().intValue();
            for (String concept : doc.getValues(FIELD_CONCEPT)) {
                int pos = concept.indexOf(':');
                if (pos > 0) {
                    md.concepts.put(concept.substring(0, pos),
                                    concept.substring(pos+1));
                }
            }
            md.doc = getXmlDoc (doc, FIELD_XML);
        }
        else {
            md = EMPTY_DOC;
        }
        return md;
    }

    public MatchedDoc getMatchedDoc (long pmid) throws Exception {
        NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange
            (FIELD_PMID, pmid, pmid, true, true);
        SearchResult result = search (query, EMPTY_FACETS);
        if (result.isEmpty())
            return EMPTY_DOC;
        
        int size = result.size();
        if (size > 1)
            Logger.warn("PMID "+pmid+" has "+size+" documents!");
        return result.docs.get(size-1);
    }

    public SearchResult search
        (Query query, Map<String, Object> facets) throws Exception {
        return search (query, facets, MAX_HITS);
    }
    
    public SearchResult search (Query query, Map<String, Object> facets,
                                int maxHits) throws Exception {
        SearchResult result = createSearchResult ();
        search (query, facets, result, maxHits);
        result.updateFacets();
        return result;
    }

    public SearchResult search (String text,
                                Map<String, Object> facets) throws Exception {
        return search (text, facets, MAX_HITS);
    }
    
    public SearchResult search (String text, Map<String, Object> facets,
                                int maxHits) throws Exception {
        QueryParser parser = new QueryParser
            (FIELD_TEXT, indexWriter.getAnalyzer());
        SearchResult result = search (parser.parse(text), facets, maxHits);
        Logger.debug("## searching for \""+text+"\" facets="+facets+"..."
                     +result.size()+" hit(s)!");
        return result;
    }

    public SearchResult search (String field, String term,
                                Map<String, Object> facets) throws Exception {
        return search (field, term, facets, MAX_HITS);
    }
    
    public SearchResult search (String field, String term,
                                Map<String, Object> facets, int maxHits)
        throws Exception {
        SearchResult result = search
            (new TermQuery (new Term (field, term)), facets, maxHits);
        Logger.debug("## searching for "+field+":"+term+"..."
                     +result.size()+" hit(s)!");
        return result;
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 3) {
            System.err.println("Usage: PubMedIndex INDEXDB MESHDB FILES...");
            System.exit(1);
        }

        Logger.debug("## IndexDb: "+argv[0]);
        Logger.debug("##  MeshDb: "+argv[1]);

        try (MeshDb mesh = new MeshDb (new File (argv[1]));
             PubMedIndex index = new PubMedIndex (new File (argv[0]))) {

            AtomicInteger count = new AtomicInteger ();
            PubMedSax pms = new PubMedSax (mesh, d -> {
                    try {
                        index.add(d);
                        Logger.debug(d.getPMID()+": "+d.getTitle());
                        if (count.incrementAndGet() > 100) {
                            return false;
                        }
                    }
                    catch (IOException ex) {
                        Logger.error("Can't index document "+d.getPMID(), ex);
                    }
                    return true;
                });
            
            for (int i = 2; i < argv.length; ++i) {
                Logger.debug("indexing "+argv[i]+"...");
                try {
                    pms.parse(new java.util.zip.GZIPInputStream
                              (new FileInputStream (argv[i])));
                }
                catch (RuntimeException ex) {
                    
                }
                index.debug();
            }
            Logger.debug("## index "+argv[0]+": "+index.size()+" document(s)!");
        }
    }

    public static class Doc {
        public static void main (String[] argv) throws Exception {
            if (argv.length < 2) {
                System.err.println
                    ("PubMedIndex$Doc INDEXDB PMIDS...");
                System.exit(1);
            }

            try (PubMedIndex index = new PubMedIndex (new File (argv[0]))) {
                for (int i = 1; i < argv.length; ++i) {
                    try {
                        long pmid = Long.parseLong(argv[i]);
                        MatchedDoc doc = index.getMatchedDoc(pmid);
                        Logger.debug("======== "+pmid+" ========");
                        if (doc != null)
                            Logger.debug(doc.toXmlString());
                    }
                    catch (Exception ex) {
                        Logger.error("Can't retrieve doc for "+argv[i], ex);
                    }
                }
            }
        }
    }
}
