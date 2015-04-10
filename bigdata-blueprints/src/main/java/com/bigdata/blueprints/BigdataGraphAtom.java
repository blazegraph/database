package com.bigdata.blueprints;

/**
 * An atomic unit of information about a property graph.  Analogous to an RDF 
 * statement- the atomic unit of information about an RDF graph.
 * 
 * @author mikepersonick
 */
public class BigdataGraphAtom {

    public static enum ElementType {
        
        VERTEX, EDGE;
        
    }
    
    /**
     * The element id.
     */
    private final String id;
    
    /**
     * The element type - vertex or edge.
     */
    private final ElementType type;
    
    /**
     * Edge from id.
     */
    private final String fromId;
    
    /**
     * Edge to id.
     */
    private final String toId;
    
    /**
     * Edge label.
     */
    private final String label;
    
    /**
     * Property key (name).
     */
    private final String key;
    
    /**
     * Property value (primitive).
     */
    private final Object val;
    
    public BigdataGraphAtom(final String id, final ElementType type,
            final String fromId, final String toId, final String label, 
            final String key, final Object val) {
        this.id = id;
        this.type = type;
        this.fromId = fromId;
        this.toId = toId;
        this.label = label;
        this.key = key;
        this.val = val;
    }

    public String getId() {
        return id;
    }

    public ElementType getType() {
        return type;
    }

    public String getFromId() {
        return fromId;
    }

    public String getToId() {
        return toId;
    }

    public String getLabel() {
        return label;
    }

    public String getKey() {
        return key;
    }

    public Object getVal() {
        return val;
    }

    @Override
    public String toString() {
        return "BigdataGraphAtom [id=" + id + ", type=" + type + ", fromId="
                + fromId + ", toId=" + toId + ", label=" + label + 
                ", key=" + key + ", val=" + val + "]";
    }

}
