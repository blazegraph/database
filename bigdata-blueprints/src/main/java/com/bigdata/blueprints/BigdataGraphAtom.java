package com.bigdata.blueprints;

/**
 * An atomic unit of information about a property graph.  Analogous to an RDF 
 * statement- the atomic unit of information about an RDF graph.
 * 
 * @author mikepersonick
 */
public abstract class BigdataGraphAtom {

    public static enum ElementType {
        
        VERTEX, EDGE;
        
    }
    
    /**
     * The element id.
     */
    protected final String id;

    protected BigdataGraphAtom(final String id) {
        this.id = id;
    }
    
    public String getId() {
        return id;
    }

    public static class ExistenceAtom extends BigdataGraphAtom {
        
        /**
         * The element type - vertex or edge.
         */
        private final ElementType type;
        
        public ExistenceAtom(final String id, final ElementType type) {
            super(id);
            
            this.type = type;
        }
        
        public ElementType getElementType() {
            return type;
        }

        @Override
        public String toString() {
            return "ExistenceAtom [id=" + id + ", type=" + type + "]";
        }
        
    }
    
    public static class EdgeAtom extends BigdataGraphAtom {
        
        /**
         * Edge from id.
         */
        private final String fromId;
        
        /**
         * Edge to id.
         */
        private final String toId;
        
        public EdgeAtom(final String id, final String fromId, final String toId) {
            super(id);
            
            this.fromId = fromId;
            this.toId = toId;
        }
        
        public String getFromId() {
            return fromId;
        }

        public String getToId() {
            return toId;
        }

        @Override
        public String toString() {
            return "EdgeAtom [id=" + id + ", from=" + fromId + ", to=" + toId + "]";
        }
        
    }
    
    public static class EdgeLabelAtom extends BigdataGraphAtom {
        
        /**
         * Edge label.
         */
        private final String label;

        public EdgeLabelAtom(final String id, final String label) {
            super(id);
            
            this.label = label;
        }
        
        public String getLabel() {
            return label;
        }

        @Override
        public String toString() {
            return "EdgeLabelAtom [id=" + id + ", label=" + label + "]";
        }
        
    }
    
    public static class PropertyAtom extends BigdataGraphAtom {
    
        /**
         * Property key (name).
         */
        private final String key;
        
        /**
         * Property value (primitive).
         */
        private final Object val;

        public PropertyAtom(final String id, final String key, final Object val) {
            super(id);
            
            this.key = key;
            this.val = val;
        }
        
        public String getKey() {
            return key;
        }

        public Object getVal() {
            return val;
        }

        @Override
        public String toString() {
            return "PropertyAtom [id=" + id + ", key=" + key + ", val=" + val + "]";
        }

    }
    
}
