package blackboard.idg;

import com.fasterxml.jackson.annotation.JsonIgnore;

public class Target extends Entity {
    public enum TDL {
        Tclin,
        Tchem,
        Tbio,
        Tdark
    }
    
    public TDL idgtdl;
    public String gene;
    public String uniprot;
    public String idgfam;
    public Double novelty;
    public String chr; // chromosome
    public Integer geneid;
    public String dtoid;
    public String stringid;
    public String sequence;
    
    public Target (long id) {
        super (id);
    }
}
