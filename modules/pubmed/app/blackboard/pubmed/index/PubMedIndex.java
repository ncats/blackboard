package blackboard.pubmed.index;

import play.Logger;
import play.libs.Json;
import play.cache.SyncCacheApi;

import blackboard.pubmed.*;
import blackboard.mesh.MeshDb;
import blackboard.mesh.MeshKSource;
import blackboard.mesh.Descriptor;
import blackboard.semmed.SemMedDbKSource;
import blackboard.semmed.Predication;
import blackboard.umls.SemanticType;
import blackboard.umls.UMLSKSource;
import blackboard.umls.index.MetaMapIndex;
import blackboard.utils.Util;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.util.zip.GZIPOutputStream;
import java.util.zip.GZIPInputStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.Callable;
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
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.search.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.suggest.document.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.shingle.ShingleAnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;

import org.apache.commons.lang3.text.WordUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringTokenizer;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.DocumentType;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import com.google.inject.assistedinject.Assisted;

public class PubMedIndex extends MetaMapIndex implements PubMedFields {
    public static final String VERSION = "PubMedIndex-v1";
    protected static final Term ALL_DOCS_TERM =
        new Term (FIELD_INDEXER, VERSION);

    static final Query MATCH_ALL = new MatchAllDocsQuery ();

    /*
     * TODO: n-gram parameters; this should be configurable somewhere!
     */
    static final int NGRAM_MIN = 2;
    static final int NGRAM_MAX = 4;
    static final int NGRAM_SIZE = Integer.MAX_VALUE;
    
    /*
     * these are internal fields
     */
    static final String _FIELD_CUI = "_cui";  // resolving concept cuis
    static final String _FIELD_CONCEPT = "_concept";
    static final String _FIELD_SEMTYPE = "_semtype";
    static final String _FACET_NGRAM = "_ngram";
        
    static final String[] HIGHLIGHT_FIELDS = {
        FIELD_TITLE,
        FIELD_AUTHOR,
        FIELD_ABSTRACT,
        FIELD_MESH,
        FIELD_KEYWORD,
        FIELD_JOURNAL,
        FIELD_GRANTID,
        FIELD_AFFILIATION
    };

    public static final Map<String, Object> EMPTY_FACETS = new HashMap<>();
    public static final MatchedDoc EMPTY_DOC = new MatchedDoc ();
    public static final SearchResult EMPTY_RESULT = new SearchResult ();

    final static Pattern RANGE_REGEX = Pattern.compile
        ("\\(([^,]*),([^\\)]*)\\)");    
    
    public static class MatchedDoc implements Comparable<MatchedDoc> {
        public Float score;
        public Long pmid;
        public String title;
        public Integer year;
        public String journal;
        public Date revised;
        public String source;
        public List<MatchedFragment> fragments = new ArrayList<>();
        public List<String> abstracts = new ArrayList<>();        
        public List<Concept> concepts = new ArrayList<>();
        public List<Concept> mesh = new ArrayList<>();

        @JsonIgnore public org.w3c.dom.Document doc;
        @JsonIgnore private final MeshDb meshdb;

        protected MatchedDoc () {
            this (null);
        }
        
        protected MatchedDoc (MeshDb meshdb) {
            this.meshdb = meshdb;
        }
        
        protected MatchedDoc (Long pmid, String title, Integer year) {
            this (null);
            this.pmid = pmid;
            this.title = title;
            this.year = year;
        }

        public PubMedDoc toDoc () {
            return PubMedDoc.getInstance(doc, meshdb);
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

        public MatchedFragment getMatchedFragment (String... fields) {
            for (MatchedFragment mf : fragments) {
                for (String f : fields) {
                    if (f.equalsIgnoreCase(mf.field))
                        return mf;
                }
            }
            return null;
        }

        public MatchedFragment getBestMatchedFragment () {
            MatchedFragment best = null;
            for (MatchedFragment mf : fragments) {
                if (best == null
                    || mf.fragment.length() > best.fragment.length())
                    best = mf;
            }
            return best;
        }

        public int compareTo (MatchedDoc d) {
            float s = d.score - score;
            if (s > 0) return 1;
            else if (s < 0) return -1;
            int dif = d.year - year;
            if (dif == 0) {
                if (d.pmid > pmid) dif = 1;
                else if (d.pmid < pmid) dif = -1;
                else if (d.revised != null)
                    dif = d.revised.compareTo(revised);
                else if (d.source != null)
                    dif = d.source.compareTo(source);
                else if (d.title != null)
                    dif = title.compareTo(d.title);
            }
            return dif;
        }
    }

    static public class PubMedTextQuery extends TextQuery {
        @JsonIgnore
        Analyzer analyzer = new StandardAnalyzer ();
        
        protected PubMedTextQuery () {
        }
        public PubMedTextQuery (Map<String, Object> facets) {
            super (facets);
        }
        public PubMedTextQuery (String query) {
            super (query);
        }
        public PubMedTextQuery (String query, Map<String, Object> facets) {
            super (query, facets);
        }
        public PubMedTextQuery (String field, String query,
                                Map<String, Object> facets) {
            super (field, query, facets);
        }
        public PubMedTextQuery (TextQuery tq) {
            super (tq);
            if (tq instanceof PubMedTextQuery) {
                setAnalyzer (((PubMedTextQuery)tq).analyzer);
            }
        }

        public void setAnalyzer (Analyzer analyzer) {
            this.analyzer = analyzer;
        }
        public Analyzer getAnalyzer () { return analyzer; }

        Query textQuery (String field, String text) throws Exception {
            String q = Util.queryRewrite(text);
            Logger.debug("** REWRITE["+field+"]: "+q);
            
            QueryParser parser = new QueryParser (field, getAnalyzer ());
            return parser.parse(q);
        }
            
        @Override
        public Query rewrite () {
            Query query = null;
            String term = (String) getQuery ();            
            if (field == null) {
                try {
                    if (term != null) {
                        query = textQuery (FIELD_TEXT, term);
                    }
                    else {
                        query = new TermQuery
                            (new Term (FIELD_INDEXER, VERSION));
                        /*
                        query = NumericRangeQuery.newLongRange
                            (FIELD_PMID, 0l, Long.MAX_VALUE, false, false);
                        */
                    }
                }
                catch (Exception ex) {
                    Logger.error("Can't parse query: "+term, ex);
                    query = new MatchNoDocsQuery ();
                }
            }
            else {
                switch (field) {
                case FIELD_PMID:
                    try {
                        long pmid = Long.parseLong(term);
                        query = NumericRangeQuery.newLongRange
                            (field, pmid, pmid, true, true);
                    }
                    catch (NumberFormatException ex) {
                        Logger.error("Bogus pmid: "+term, ex);
                    }
                    break;
                    
                case FIELD_YEAR:
                    try {
                        // syntax: (XXXX,YYYY) all years from XXXX to YYYY
                        //      inclusive
                        //   (,YYYY) all years before YYYY inclusive
                        //   (XXXX,) all years after XXXX inclusive
                        //   ZZZZ only year ZZZZ
                        int min = Integer.MIN_VALUE, max = Integer.MAX_VALUE; 
                        Matcher m = RANGE_REGEX.matcher(term);
                        if (m.find()) {
                            String m1 = m.group(1);
                            if (!m1.equals(""))
                                min = Integer.parseInt(m1);
                            String m2 = m.group(2);
                            if (!m2.equals(""))
                                max = Integer.parseInt(m2);
                            Logger.debug("### year range search: "
                                         +" min="+m1+" max="+m2);
                        }
                        else {
                            min = max = Integer.parseInt(term);
                        }
                        query = NumericRangeQuery.newIntRange
                            (field, min, max, true, true);
                    }
                    catch (NumberFormatException ex) {
                        Logger.error("Bogus year format: "+term, ex);
                    }
                    break;
                    
                default:
                    try {
                        if ('_' == field.charAt(0)) {
                            // internal field, so search as-is
                            query = new TermQuery (new Term (field, term));
                        }
                        else {
                            query = textQuery (field, term);
                        }
                    }
                    catch (Exception ex) {
                        Logger.error("Bogus syntax: "+term
                                     +"; revert to phrase query", ex);
                        query = new QueryBuilder (getAnalyzer ())
                            .createPhraseQuery(field, term, slop);
                    }
                }
            }
            
            if (query == null)
                query = new MatchNoDocsQuery ();
            
            return query;
        }
    } // PubMedTextQuery 
    
    public static class PMIDQuery implements SearchQuery {
        public final Long pmid;
        public PMIDQuery (Long pmid) {
            this.pmid = pmid;
        }

        public String getField () { return FIELD_PMID; }
        public Object getQuery () { return pmid; }
        public Map<String, Object> getFacets () {
            return Collections.emptyMap();
        }
        public int max () { return 5; }
        public List<Concept> getConcepts () {
            return Collections.emptyList();
        }
        @Override
        public Query rewrite () {
            return NumericRangeQuery.newLongRange
                (getField (), pmid, pmid, true, true);
        }
        
        public String cacheKey () {
            return PMIDQuery.class.getName()+"/"+pmid;
        }

        public String toString () {
            return "PMIDQuery{pmid="+pmid+"}";
        }
    }

    public static class PMIDBatchQuery implements SearchQuery {
        public final Set<Long> pmids = new LinkedHashSet<>();
        public PMIDBatchQuery (long[] pmids) {
            for (int i = 0; i < pmids.length; ++i)
                this.pmids.add(pmids[i]);
        }
        public PMIDBatchQuery (Long... pmids) {
            for (int i = 0; i < pmids.length; ++i)
                this.pmids.add(pmids[i]);
        }
        public PMIDBatchQuery add (Long id) {
            pmids.add(id);
            return this;
        }
        public String getField () { return FIELD_PMID; }
        public Object getQuery () {
            return pmids.toArray(new Long[0]);
        }
        public int top () { return pmids.size(); }
        public int max () { return pmids.size(); }
        public Map<String, Object> getFacets () {
            return Collections.emptyMap();
        }
        public List<Concept> getConcepts () {
            return Collections.emptyList();
        }
        public String cacheKey () {
            return PMIDBatchQuery.class.getName()
                +"/"+Util.sha1(pmids.toArray(new Long[0]));
        }
        
        @Override
        public Query rewrite () {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Long id : pmids) {
                builder.add(NumericRangeQuery.newLongRange
                            (getField (), id, id, true, true),
                            BooleanClause.Occur.SHOULD);
            }
            return builder.build();
        }
    }
    
    public static class SearchResult
        extends blackboard.index.Index.SearchResult {
        public final List<MatchedDoc> docs = new ArrayList<>();
        
        final MeshDb mesh;
        final UMLSKSource umls;
        final SyncCacheApi cache;
        
        protected SearchResult () {
            this (null, null, null, null);
        }

        protected SearchResult (SearchQuery query) {
            this (query, null, null, null);
        }

        protected SearchResult (SearchResult result) {
            super (result);
            mesh = result.mesh;
            umls = result.umls;
            cache = result.cache;
        }
        
        protected SearchResult (SearchQuery query, SyncCacheApi cache,
                                MeshKSource mesh, UMLSKSource umls) {
            super (query);
            this.mesh = mesh != null ? mesh.getMeshDb() : null;
            this.umls = umls;
            this.cache = cache;
        }

        public SearchResult page (int skip, int top) {
            if (skip > docs.size())
                throw new IllegalArgumentException
                    ("Can't skip beyond total number documents!");
            int max = Math.min(docs.size(), skip+top);
            SearchResult result = new SearchResult (this);
            result.docs.addAll(docs.subList(skip, max));
            return result;
        }
        
        @Override
        public int size () { return docs.size(); }

        @Override
        public SearchResult clone () {
            SearchResult result = new SearchResult (this);
            result.facets.clear(); // clone facets instead of referencing
            for (Facet f : facets) {
                result.facets.add(PubMedIndex.clone(f));
            }
            return result;
        }
        
        protected boolean process (IndexSearcher searcher,
                                   blackboard.index.Index.ResultDoc rdoc) {
            try {
                MatchedDoc mdoc = toMatchedDoc (rdoc.doc, mesh);
                String[] frags = rdoc.getFragments(FIELD_TEXT, 500, 10);
                if (frags != null && frags.length > 0) {
                    for (String f : frags)
                        mdoc.fragments.add(new MatchedFragment (f));
                }
                else {
                    for (String field : HIGHLIGHT_FIELDS) {
                        frags = rdoc.getFragments(field, 500, 10);
                        if (frags != null && frags.length > 0) {
                            for (String f : frags)
                                mdoc.fragments.add
                                    (new MatchedFragment (field, f));
                            break;
                        }
                    }
                }
                
                for (String cui : rdoc.doc.getValues(FIELD_CUI)) {
                    Concept c = getConcept (searcher, cui);
                    if (c != null)
                        mdoc.concepts.add(c);
                }

                for (String ui : rdoc.doc.getValues(FIELD_UI)) {
                    Descriptor d = (Descriptor)mesh.getEntry(ui);
                    if (d != null) {
                        Concept c = new Concept
                            (d.ui, d.name, d.treeNumbers.isEmpty() ? null
                             : d.treeNumbers.get(0));
                        for (int i = 1; i < d.treeNumbers.size(); ++i)
                            c.types.add(d.treeNumbers.get(i));
                        mdoc.mesh.add(c);
                    }
                }
                
                mdoc.score = rdoc.score;
                docs.add(mdoc);
            }
            catch (IOException ex) {
                Logger.error("Can't process doc "+rdoc.docId, ex);
            }
            return true;
        }

        @Override
        protected void postProcessing (IndexSearcher searcher)
            throws IOException {
            for (Facet f : facets) {
                switch (f.name) {
                case FACET_UI:
                case FACET_PUBTYPE:
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

                case FACET_SEMTYPE:
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

                case FACET_CUI:
                    for (FV fv : f.values) {
                        Concept concept = getConcept (searcher, fv.label);
                        if (concept != null)
                            fv.display = concept.name;
                    }
                    break;

                default:
                    if (f.name.startsWith(FACET_TR) && mesh != null) {
                        for (FV fv : f.values)
                            updateTreeNumberDisplay (fv);
                    }
                    break;
                }
            }
        } // postProcessing ()

        Concept getConcept (final IndexSearcher searcher, final String cui) {
            return cache.getOrElseUpdate("PubMedIndex/concept/"+cui, () -> {
                    try {
                        return _getConcept (searcher, cui);
                    }
                    catch (IOException ex) {
                        Logger.error("Can't get concept: "+cui, ex);
                    }
                    return null;
                });
        }
        
        Concept _getConcept (IndexSearcher searcher, String cui)
            throws IOException {
            TermQuery tq = new TermQuery (new Term (_FIELD_CUI, cui));
            TopDocs hits = searcher.search(tq, 1);
            Concept c = null;
            if (hits.totalHits > 0) {
                Document doc = searcher.doc(hits.scoreDocs[0].doc);
                String[] types = doc.getValues(_FIELD_SEMTYPE);
                c = new Concept
                    (doc.get(_FIELD_CUI), doc.get(_FIELD_CONCEPT), types[0]);
                for (int i = 1; i < types.length; ++i)
                    c.types.add(types[i]);
            }
            return c;
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
            PubMedIndex.exportXML(docs, new StreamResult (os));
        }
        public String exportXML () throws Exception {
            StringWriter writer = new StringWriter ();
            PubMedIndex.exportXML(docs, new StreamResult (writer));
            return writer.toString();
        }
    }

    public static void exportXML (List<MatchedDoc> docs, StreamResult out)
        throws Exception {
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
            .transform(new DOMSource (container), out);
    }

    @Inject public SemMedDbKSource semmed;
    @Inject public MeshKSource mesh;
    @Inject public UMLSKSource umls;
    @Inject SyncCacheApi cache;
    
    @Inject
    public PubMedIndex (@Assisted File dir) throws IOException {
        super (dir);
    }

    @Override
    protected FacetsConfig configFacets () {
        FacetsConfig fc = new FacetsConfig ();
        fc.setMultiValued(FACET_TR, true);
        fc.setHierarchical(FACET_TR, true);
        for (String cat : TR_CATEGORIES) {
            fc.setMultiValued(FACET_TR+cat, true);
            fc.setHierarchical(FACET_TR+cat, true);
        }
        fc.setMultiValued(FACET_UI, true);
        fc.setMultiValued(FACET_SEMTYPE, true); // umls semantic types
        fc.setMultiValued(FACET_SOURCE, true); // umls sources
        fc.setMultiValued(FACET_CUI, true);
        fc.setMultiValued(FACET_PREDICATE, true);
        fc.setMultiValued(FACET_AUTHOR, true);
        fc.setMultiValued(FACET_INVESTIGATOR, true);
        fc.setMultiValued(FACET_ORCID, true);
        fc.setMultiValued(FACET_PUBTYPE, true);
        fc.setMultiValued(FACET_JOURNAL, true);
        fc.setMultiValued(FACET_KEYWORD, true);
        fc.setMultiValued(FACET_LANG, true);
        fc.setMultiValued(FACET_REFERENCE, true);
        fc.setMultiValued(FACET_GRANTTYPE, true);
        fc.setHierarchical(FACET_GRANTAGENCY, true);
        fc.setMultiValued(FACET_GRANTAGENCY, true);
        fc.setMultiValued(FACET_GRANTCOUNTRY, true);
        fc.setMultiValued(_FACET_NGRAM, true);
        
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

    public int deleteDocs (Long...pmids) throws IOException {
        int start = indexWriter.numDocs();
        for (Long id : pmids) {
            Query q = NumericRangeQuery.newLongRange
                (FIELD_PMID, id, id, true, true);
            indexWriter.deleteDocuments(q);
        }
        indexWriter.commit();
        return start - indexWriter.numDocs();
    }
    
    public boolean addIfAbsent (PubMedDoc d) throws IOException {
        if (!indexed (d.getPMID())) {
            add (d);
            return true;
        }
        return false;
    }

    public int deleteDocsIfOlderThan (PubMedDoc... docs) throws IOException {
        int start = indexWriter.numDocs();
        for (PubMedDoc d : docs) {
            long revised = d.revised.getTime();
            BooleanQuery.Builder builder = new BooleanQuery.Builder()
                .add(NumericRangeQuery.newLongRange
                     (FIELD_PMID, d.pmid, d.pmid, true, true),
                     BooleanClause.Occur.MUST)
                .add(NumericRangeQuery.newLongRange
                     (FIELD_REVISED, 0l, revised, false, false),
                     BooleanClause.Occur.MUST);
            indexWriter.deleteDocuments(builder.build());
        }
        indexWriter.commit();
        return start - indexWriter.numDocs();
    }

    /*
     * return PubMedDoc's for which there don't exist newer versions
     * already indexed
     */
    public PubMedDoc[] checkDocsIfNewerThan (PubMedDoc... docs)
        throws IOException {
        List<PubMedDoc> newests = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            for (PubMedDoc d : docs) {
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                builder.add(NumericRangeQuery.newLongRange
                            (FIELD_PMID, d.pmid, d.pmid, true, true),
                            BooleanClause.Occur.MUST);
                long revised = d.revised.getTime();
                builder.add(NumericRangeQuery.newLongRange
                            (FIELD_REVISED, revised, Long.MAX_VALUE,
                             true, false), BooleanClause.Occur.MUST);
                TopDocs hits = searcher.search(builder.build(), 1);
                if (hits.totalHits == 0) {
                    //Logger.debug(d.pmid+"... to be added");
                    newests.add(d);
                }
            }
        }
        return newests.toArray(new PubMedDoc[0]);
    }
    
    protected Document instrument (Document doc, PubMedDoc d)
        throws IOException {
        doc.add(new LongField (FIELD_PMID, d.getPMID(), Field.Store.YES));
        addTextField (doc, FIELD_PMID, d.getPMID());
        if (d.timestamp != null)
            doc.add(new LongField (FIELD_TIMESTAMP,
                                   d.timestamp, Field.Store.YES));
        if (d.source != null) {
            doc.add(new FacetField (FACET_FILE, d.source));
            doc.add(new StringField (FIELD_FILE, d.source, Field.Store.YES));
        }
        
        String title = d.getTitle();
        if (title != null && !"".equals(title)) {
            doc.add(new Field (FIELD_TITLE, title, tvFieldType));
            addTextField (doc, FIELD_TITLE, title);
            if (title.length() < 1024) {
                Set<String> ngrams = ngrams (title, NGRAM_MIN, NGRAM_MAX);
                for (String s : ngrams) {
                    if (s.length() > 2) {
                        doc.add(new FacetField (_FACET_NGRAM, s));
                        //Logger.debug("...\""+s+"\"");
                    }
                }
                Logger.debug("## N-Gram: "+title+"..."+ngrams.size());
            }
        }

        // author
        for (PubMedDoc.Author auth : d.authors) {
            if (auth.affiliations != null) {
                for (String affi : auth.affiliations) {
                    addTextField (doc, FIELD_AFFILIATION, affi);
                    doc.add(new Field (FIELD_AFFILIATION, affi, tvFieldType));
                }
            }
            if (auth.identifier != null
                && auth.identifier.indexOf("orcid") > 0) {
                int pos = auth.identifier.lastIndexOf('/');
                if (pos > 0) {
                    String orcid = auth.identifier.substring(pos+1);
                    if (!"".equals(orcid.trim())) {
                        doc.add(new FacetField (FACET_ORCID, orcid));
                    }
                }
                doc.add(new Field (FIELD_ORCID, auth.identifier, tvFieldType));
            }
            // facet-based search
            doc.add(new FacetField (FACET_AUTHOR, auth.getName()));
            // field-based search
            doc.add(new Field (FIELD_AUTHOR, auth.getName(), tvFieldType));
            // for general text search
            addTextField (doc, FIELD_AUTHOR, auth.getName());
        }

        for (PubMedDoc.Author auth : d.investigators) {
            doc.add(new FacetField (FACET_INVESTIGATOR, auth.getName()));
            doc.add(new Field (FIELD_INVESTIGATOR,
                               auth.getName(), tvFieldType));
            addTextField (doc, FIELD_INVESTIGATOR, auth.getName());
        }

        // journal
        if (d.journal != null && d.journal.length() > 0) {
            doc.add(new FacetField (FACET_JOURNAL, d.journal));
            doc.add(new Field (FIELD_JOURNAL, d.journal, tvFieldType));
        }

        // grants
        for (PubMedDoc.Grant grant : d.grants) {
            if (grant.type != null && grant.type.length() > 0)
                doc.add(new FacetField (FACET_GRANTTYPE, grant.type));

            if (grant.id != null) {
                String[] toks = grant.id.split(",");
                for (String t : toks) {
                    String id = t.trim();
                    if (id.length() > 0) {
                        doc.add(new Field (FIELD_GRANTID, id, tvFieldType));
                        addTextField (doc, FIELD_GRANTID, id);
                    }
                }
            }
            
            if (grant.agency != null && grant.agency.length() > 0) {
                if (grant.agency.indexOf('|') > 0) {
                    String[] toks = grant.agency.split("\\|");
                    for (int i = 0; i < toks.length; ++i)
                        toks[i] = toks[i].trim();
                    doc.add(new FacetField (FACET_GRANTAGENCY, toks));
                }
                else if (grant.agency.endsWith("HHS")) {
                    String[] toks = grant.agency.split("[\\s]+");
                    for (int i = 0, j = toks.length-1; i < j; ++i, --j) {
                        String t = toks[i];
                        toks[i] = toks[j];
                        toks[j] = t;
                    }
                    if (toks.length > 0)
                        doc.add(new FacetField (FACET_GRANTAGENCY, toks));
                }
                else
                    doc.add(new FacetField (FACET_GRANTAGENCY, grant.agency));
            }
            
            if (grant.country != null && grant.country.length() > 0)
                doc.add(new FacetField (FACET_GRANTCOUNTRY, grant.country));
        }

        // publication types
        for (blackboard.mesh.Entry e : d.pubtypes) {
            doc.add(new FacetField (FACET_PUBTYPE, e.ui));
            doc.add(new Field (FIELD_PUBTYPE, e.name, tvFieldType));
        }

        // keywords
        for (String k : d.keywords) {
            String keyword = WordUtils.capitalize(k);
            doc.add(new FacetField (FACET_KEYWORD, keyword));
            doc.add(new Field (FIELD_KEYWORD, k, tvFieldType));
            addTextField (doc, FIELD_KEYWORD, k);
        }

        // abstract texts
        for (String abs : d.getAbstract()) {
            doc.add(new Field (FIELD_ABSTRACT, abs, tvFieldType));
            addTextField (doc, FIELD_ABSTRACT, abs);
        }

        if (d.lang != null && !"".equals(d.lang)) {
            doc.add(new FacetField (FACET_LANG, d.lang));
        }

        // publication year
        doc.add(new IntField (FIELD_YEAR, d.getYear(), Field.Store.YES));
        doc.add(new FacetField (FACET_YEAR, String.valueOf(d.getYear())));

        if (d.revised != null) {
            doc.add(new LongField
                    (FIELD_REVISED, d.revised.getTime(), Field.Store.YES));
        }

        // mesh headings
        Logger.debug("## MeSH headings...");
        for (MeshHeading mh : d.getMeshHeadings()) {
            Descriptor desc = (Descriptor)mh.descriptor;
            doc.add(new FacetField (FACET_UI, desc.ui));
            doc.add(new Field (FIELD_UI, desc.ui, tvFieldType));
            addTextField (doc, FIELD_UI, desc.ui);

            for (String tr : desc.treeNumbers) {
                Logger.debug("..."+tr);
                String[] path = tr.split("\\.");
                doc.add(new FacetField (FACET_TR, path));
                doc.add(new FacetField
                        (FACET_TR+path[0].substring(0, 1), path));
                doc.add(new Field (FIELD_TR, tr, tvFieldType));
            }
            
            doc.add(new Field (FIELD_MESH, desc.name, tvFieldType));
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
                for (Long id : ref.pmids) {
                    String s = id.toString();
                    doc.add(new FacetField (FACET_REFERENCE, s));
                    doc.add(new StringField
                            (FIELD_REFERENCE, s, Field.Store.NO));
                }
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
                semmed (doc, d.getPMID());
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

    void semmed (Document doc, Long pmid) throws Exception {
        List<Predication> preds =
            semmed.getPredicationsByPMID(pmid.toString());

        if (!preds.isEmpty()) {
            Map<String, String> cuis = new LinkedHashMap<>();
            Set<String> types = new LinkedHashSet<>();
            Set<String> predicates = new LinkedHashSet<>();
            
            try (IndexReader reader = DirectoryReader.open(indexWriter)) {
                IndexSearcher searcher = new IndexSearcher (reader);
                for (Predication p : preds) {
                    addConceptIfAbsent
                        (searcher, new Concept (p.subcui,
                                                p.subject, p.subtype));
                    addConceptIfAbsent
                        (searcher, new Concept (p.objcui,
                                                 p.object, p.objtype));
                    cuis.put(p.subcui, p.subject);
                    cuis.put(p.objcui, p.object);
                    types.add(p.subtype);
                    types.add(p.objtype);
                    predicates.add(p.predicate);
                }
            }
            
            for (Map.Entry<String, String> me : cuis.entrySet()) {
                doc.add(new FacetField (FACET_CUI, me.getKey()));
                doc.add(new Field (FIELD_CUI, me.getKey(), tvFieldType));
                addTextField (doc, FIELD_CUI, me.getKey());
                addTextField (doc, FIELD_CONCEPT, " cui=\""
                              +me.getKey()+"\"", me.getValue());
            }
            for (String type : types)
                doc.add(new FacetField (FACET_SEMTYPE, type));
            for (String pred : predicates)
                doc.add(new FacetField (FACET_PREDICATE, pred));
        }
    }

    void addConceptIfAbsent (IndexSearcher searcher, Concept concept) 
        throws IOException {
        TermQuery tq = new TermQuery (new Term (_FIELD_CUI, concept.ui));
        TopDocs hits = searcher.search(tq, 1);
        if (hits.totalHits > 0) {
            //searcher.doc(hits.scoreDocs[0].doc);
        }
        else {
            Document doc = new Document ();
            doc.add(new StringField
                    (_FIELD_CUI, concept.ui, Field.Store.YES));
            doc.add(new TextField
                    (_FIELD_CONCEPT, concept.name, Field.Store.YES));
            for (String t : concept.types)
                doc.add(new StringField (_FIELD_SEMTYPE, t, Field.Store.YES));
            indexWriter.addDocument(doc);
        }
    }       

    protected Document newDocument () {
        Document doc = new Document ();
        doc.add(new StringField
                (FIELD_INDEXER, VERSION, Field.Store.NO));
        return doc;
    }
    
    public PubMedIndex add (PubMedDoc d) throws IOException {
        Logger.debug(Thread.currentThread().getName()+": "
                     +d.getPMID()+": "+d.getTitle());
        add (instrument (newDocument (), d));
        return this;
    }

    public void debug () throws IOException {
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
            FacetResult fr = facets.getTopChildren(20, FACET_UI);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FACET_CUI);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FACET_SEMTYPE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FACET_SOURCE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FACET_AUTHOR);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FACET_REFERENCE);
            if (fr != null)
                Logger.debug(fr.toString());
            fr = facets.getTopChildren(20, FACET_KEYWORD);
            if (fr != null)
                Logger.debug(fr.toString());

            IOUtils.close(taxonReader);
        }
        finally {
            if (searcher != null)
                IOUtils.close(searcher.getIndexReader());
        }
    }

    SearchResult createSearchResult (SearchQuery query) {
        return new SearchResult (query, cache, mesh, umls);
    }

    static MatchedDoc toMatchedDoc (Document doc) throws IOException {
        return toMatchedDoc (doc, null);
    }
    
    static MatchedDoc toMatchedDoc (Document doc, MeshDb mesh)
        throws IOException {
        return toMatchedDoc (new MatchedDoc (mesh), doc);
    }
    
    static MatchedDoc toMatchedDoc (MatchedDoc md, Document doc)
        throws IOException {
        IndexableField field = doc.getField(FIELD_PMID);
        if (field != null) {
            md.pmid = field.numericValue().longValue();
            md.title = doc.get(FIELD_TITLE);
            md.year = doc.getField(FIELD_YEAR).numericValue().intValue();
            md.journal = doc.get(FIELD_JOURNAL);
            md.doc = getXmlDoc (doc, FIELD_XML);
            for (String txt : doc.getValues(FIELD_ABSTRACT))
                md.abstracts.add(txt);
            md.source = doc.get(FIELD_FILE);
            IndexableField f = doc.getField(FIELD_REVISED);
            if (f != null) {
                md.revised = new Date (f.numericValue().longValue());
            }
        }
        else {
            md = EMPTY_DOC;
        }
        return md;
    }

    public MatchedDoc[] getMatchedDocs (long pmid) throws Exception {
        PMIDQuery pq = new PMIDQuery (pmid);
        SearchResult result = search (pq);
        if (result.total == 0)
            return new MatchedDoc[0];
        MatchedDoc[] docs = result.docs.toArray(new MatchedDoc[0]);
        if (docs.length > 1) {
            Arrays.sort(docs);
        }
        return docs;
    }
        
    public MatchedDoc getMatchedDoc (long pmid) throws Exception {
        MatchedDoc[] docs = getMatchedDocs (pmid);
        if (docs.length > 1)
            Logger.warn("PMID "+pmid+" has "+docs.length+" documents!");
        return docs.length == 0 ? EMPTY_DOC : docs[0];
    }

    public SearchResult facets (final SearchQuery query) throws Exception {
        final String key = root.getName()+"/facets/"+query.cacheKey();
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                SearchResult result = createSearchResult (query);
                facets (result);
                return result;
            });
    }
    
    public SearchResult search (final SearchQuery query) throws Exception {
        final String key = root.getName()+"/search/"+query.cacheKey();
        return cache.getOrElseUpdate(key, () -> {
                Logger.debug("Cache missed: "+key);
                SearchQuery q = query;
                if (query instanceof TextQuery) {
                    // we recast this to use PubMedTextQuery
                    q = new PubMedTextQuery ((TextQuery)query);
                }
                SearchResult result = createSearchResult (q);
                search (result, q.max());
                return result;
            });
    }
    
    public SearchResult search (String text,
                                Map<String, Object> facets) throws Exception {
        return search (text, facets, MAX_HITS);
    }
    
    public SearchResult search (String text, Map<String, Object> facets,
                                int maxHits) throws Exception {
        PubMedTextQuery query = new PubMedTextQuery (text, facets);
        query.top = maxHits;
        SearchResult result = search (query);
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
        PubMedTextQuery query = new PubMedTextQuery (field, term, facets);
        query.top = maxHits;
        SearchResult result = search (query);
        Logger.debug("## searching for "+field+":"+term+"..."
                     +result.size()+" hit(s)!");
        return result;
    }

    public TermVector termVector (String field) throws IOException {
        return termVector (field, ALL_DOCS_TERM);
    }

    public TermVector termVector (String field, SearchQuery query)
        throws IOException {
        if (query != null)
            return termVector (field, query.rewrite());
        return termVector (field);
    }

    Query toQuery (SearchQuery query) {
        Query q = query.rewrite();
        if (query instanceof FacetQuery) {
            DrillDownQuery ddq = ((FacetQuery)query)
                .getFacetQuery(facetConfig, q);
            if (ddq != null) q = ddq;
        }
        return q;
    }
    
    public Map<String, Integer> getNgrams (SearchQuery query)
        throws IOException {
        return getNgrams (toQuery (query));
    }

    public Map<String, Integer> getNgrams (Query query) throws IOException {
        return getNgrams (new TreeMap<>(), query);
    }
    
    public Map<String, Integer> getNgrams (Map<String, Integer> ngrams,
                                           Query query) throws IOException {    
        long start = System.currentTimeMillis();
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxon = new DirectoryTaxonomyReader (taxonWriter)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            FacetsCollector fc = new FacetsCollector ();

            // don't care about the returned documents..            
            TopDocs hits = FacetsCollector.search(searcher, query, 1, fc);
            Facets facets = new FastTaxonomyFacetCounts(taxon, facetConfig, fc);
            
            FacetResult fr = facets.getTopChildren(NGRAM_SIZE, _FACET_NGRAM);
            if (fr != null) {
                for (int i = 0; i < fr.labelValues.length; ++i) {
                    LabelAndValue lv = fr.labelValues[i];
                    int count = lv.value.intValue();
                    Integer c = ngrams.get(lv.label);
                    ngrams.put(lv.label, c == null ? count : (c+count));
                }
            }
            Logger.debug("### N-grams "+query+" => "
                         +hits.totalHits+" doc(s)..."+ngrams.size()+" in "
                         +String.format
                         ("%1$.3fs", (System.currentTimeMillis()-start)*1e-3));
        }
        
        return ngrams;
    }

    public Map<String, Integer> andNotNgrams (Map<String, Integer> ngrams,
                                              SearchQuery q,
                                              SearchQuery p)
        throws IOException {
        return andNotNgrams (ngrams, toQuery (q), toQuery (p));
    }

    public Map<String, Integer> andNotNgrams (SearchQuery q, SearchQuery p)
        throws IOException {
        return andNotNgrams (toQuery (q), toQuery (p));
    }

    public Map<String, Integer> andNotNgrams (Query q, Query p)
        throws IOException {
        return andNotNgrams (new TreeMap<>(), q, p);
    }

    public Map<String, Integer> andNotNgrams
        (Map<String, Integer> ngrams, Query q, Query p) throws IOException {
        Query andNot = new BooleanQuery.Builder()
            .add(q, BooleanClause.Occur.MUST)
            .add(p, BooleanClause.Occur.MUST_NOT)
            .build();
        return getNgrams (ngrams, andNot);
    }

    public Map<String, Integer> andNgrams (SearchQuery q, SearchQuery p)
        throws IOException {
        return andNgrams (new TreeMap<>(), toQuery (q), toQuery (p));
    }
    
    public Map<String, Integer> andNgrams (Map<String, Integer> ngrams,
                                           SearchQuery q, SearchQuery p)
        throws IOException {
        return andNgrams (ngrams, toQuery (q), toQuery (p));
    }
    
    public Map<String, Integer> andNgrams (Query q, Query p)
        throws IOException {
        return andNgrams (new TreeMap<>(), q, p);
    }
    
    public Map<String, Integer> andNgrams
        (Map<String, Integer> ngrams, Query q, Query p) throws IOException {
        Query and = new BooleanQuery.Builder()
            .add(q, BooleanClause.Occur.MUST)
            .add(p, BooleanClause.Occur.MUST)
            .build();
        return getNgrams (ngrams, and);
    }
    
    public Map<String, Integer> getValuesForFacet
        (Map<String, Integer> values, String facet) throws IOException {
        long start = System.currentTimeMillis();
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxon = new DirectoryTaxonomyReader (taxonWriter)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            FacetsCollector fc = new FacetsCollector ();
            TopDocs hits = FacetsCollector.search(searcher, MATCH_ALL, 1, fc);
            Facets facets = new FastTaxonomyFacetCounts(taxon, facetConfig, fc);
            FacetResult fr = facets.getTopChildren(Integer.MAX_VALUE, facet);
            if (fr != null) {
                for (int i = 0; i < fr.labelValues.length; ++i) {
                    LabelAndValue lv = fr.labelValues[i];
                    int count = lv.value.intValue();
                    Integer c = values.get(lv.label);
                    values.put(lv.label, c == null ? count : (c+count));
                }
            }
        }
        Logger.debug("### Total facet["+facet+"] counts executed in "
                     +String.format
                     ("%1$.3fs", (System.currentTimeMillis()-start)*1e-3)
                     +"..."+values.size());
        return values;
    }

    public List<FV> getNgramFacetValues (SearchQuery query) throws IOException {
        Map<String, Integer> ngrams = getNgrams (query);
        Map<Object, Integer> counts = getCountsForFacetValues
            (new TreeMap<>(), _FACET_NGRAM, ngrams.keySet());
        List<FV> values = new ArrayList<>();
        for (Map.Entry<String, Integer> me : ngrams.entrySet()) {
            Integer total = counts.get(me.getKey());
            if (total != null) {
                FV fv = new FV (me.getKey(), me.getValue());
                fv.total = total;
                values.add(fv);
            }
            else {
                Logger.warn("Couldn't get total count for N-gram \""
                            +me.getKey()+"\"!");
            }
        }
        return values;
    }

    public Map<Object, Integer> getCountsForNgramValues
        (Collection values) throws IOException {
        return getCountsForNgramValues (new TreeMap<>(), values);
    }
    
    public Map<Object, Integer> getCountsForNgramValues
        (Map<Object, Integer> counts, Collection values) throws IOException {
        return getCountsForFacetValues (counts, _FACET_NGRAM, values);
    }
    
    public Map<Object, Integer> getCountsForFacetValues
        (Map<Object, Integer> counts, String facet, Collection values)
        throws IOException {
        long start = System.currentTimeMillis();
        try (IndexReader reader = DirectoryReader.open(indexWriter);
             TaxonomyReader taxon = new DirectoryTaxonomyReader (taxonWriter)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            FacetsCollector fc = new FacetsCollector ();
            TopDocs hits = FacetsCollector.search(searcher, MATCH_ALL, 1, fc);
            Facets facets = new FastTaxonomyFacetCounts(taxon, facetConfig, fc);
            for (Object v : values) {
                Number cnt = null;
                if (v.getClass().isArray()) {
                    cnt = facets.getSpecificValue(facet, (String[])v);
                }
                else if (v instanceof String) {
                    cnt = facets.getSpecificValue(facet, (String)v);
                }
                else {
                }

                if (cnt != null) {
                    Integer c = counts.get(v);
                    counts.put(v, c == null
                               ? cnt.intValue() : (c+cnt.intValue()));
                }
            }
        }
        Logger.debug("### Total facet["+facet+"] counts executed in "
                     +String.format
                     ("%1$.3fs", (System.currentTimeMillis()-start)*1e-3)
                     +"..."+counts.size());
        return counts;
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
            PubMedSax pms = new PubMedSax (mesh, (s, d) -> {
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
