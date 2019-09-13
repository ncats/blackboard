package blackboard.pubmed.index;

import java.util.*;
import blackboard.index.Index;
import static blackboard.pubmed.index.PubMedIndex.*;

public class Analytics {
    protected Analytics () {
    }
    
    public static SearchResult merge (SearchResult... results) {
        Map<String, List<Facet>> facets = new TreeMap<>();
        List<MatchedDoc> docs = new ArrayList<>();
        SearchResult merged = null;
        if (results.length > 0) {
            for (SearchResult r : results) {
                docs.addAll(r.docs);
                if (merged == null) {
                    // this assumes that all SearchResults came from the same
                    // SearchQuery!
                    merged = new SearchResult (r.query);
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
        }
        else {
            merged = new SearchResult ();
        }

        int thres = (int)(merged.total * .4+0.5);
        for (List<Facet> fs : facets.values()) {
            Facet f = Index.merge(fs.toArray(new Facet[0]));
            // only show facet values that are at most 40% of the total count
            f.trim(thres, merged.query.fdim());
            merged.facets.add(f);
        }

        /*
        Logger.debug("$$$$$$ merged facets...");
        for (Facet f : merged.facets)
            Logger.debug(f.toString());
        */
        
        Collections.sort(docs);
        merged.docs.addAll(docs);
        return merged;
    }
}
