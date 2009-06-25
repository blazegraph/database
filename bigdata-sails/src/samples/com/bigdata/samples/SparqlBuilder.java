package com.bigdata.samples;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

public class SparqlBuilder {
    private Graph where, construct;

    public SparqlBuilder() {
        where = new Graph();
        construct = new Graph();
    }

    public SparqlBuilder(String s, String p, String o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(String s, String p, Literal o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(String s, String p, URI o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(String s, String p, Value o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(String s, URI p, String o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(String s, URI p, Literal o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(String s, URI p, URI o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(String s, URI p, Value o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(URI s, String p, String o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(URI s, String p, Literal o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(URI s, String p, URI o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(URI s, String p, Value o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder(URI s, URI p, String o) {
        this();
        addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder reset() {
        where = new Graph();
        construct = new Graph();
        return this;
    }

    public Graph getWhere() {
        return where;
    }

    public Graph getConstruct() {
        return construct;
    }

    public static String var(String s) {
        return s;
    }

    public static String uri(String s) {
        return "<" + s + ">";
    }

    public static String uri(URI uri) {
        return uri(uri.stringValue());
    }

    public static String literal(String s) {
        return "\"" + s + "\"";
    }

    public static String literal(Literal l) {
        return literal(l.stringValue());
    }

    public SparqlBuilder addTriplePattern(String s, String p, String o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, String p, Literal o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, String p, URI o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, String p, Value o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, URI p, String o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, URI p, Literal o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, URI p, URI o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, URI p, Value o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(URI s, String p, String o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(URI s, String p, Literal o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(URI s, String p, URI o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(URI s, String p, Value o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(URI s, URI p, String o) {
        return addTriplePattern(s, p, o, true);
    }

    public SparqlBuilder addTriplePattern(String s, String p, String o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(String s, String p, Literal o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(String s, String p, URI o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(String s, String p, Value o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(String s, URI p, String o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(String s, URI p, Literal o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(String s, URI p, URI o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(String s, URI p, Value o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(URI s, String p, String o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(URI s, String p, Literal o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(URI s, String p, URI o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(URI s, String p, Value o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder addTriplePattern(URI s, URI p, String o,
            boolean construct) {
        where.addTriplePattern(s, p, o);
        if (construct) {
            this.construct.addTriplePattern(s, p, o);
        }
        return this;
    }

    public SparqlBuilder setFilter(String filter) {
        where.setFilter(filter);
        return this;
    }

    private static String CONSTRUCT = "construct ".intern();

    private static String WHERE = " where ".intern();

    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (where.isEmpty() || construct.isEmpty()) {
            return sb.toString();
        }
        sb.append(CONSTRUCT).append(construct).append(WHERE).append(where);
        return sb.toString();
    }

    public static interface Pattern {
    }

    public static class UnionGraph extends LinkedList<Graph> implements
            Collection<Graph>, Pattern {
        private static final long serialVersionUID = -1L;

        public static final String UNION = " UNION ".intern();

        public Graph addGraph() {
            Graph g = new Graph();
            add(g);
            return g;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (isEmpty()) {
                return sb.toString();
            }
            Iterator<Graph> it = iterator();
            while (it.hasNext()) {
                sb.append(it.next().toString());
                if (it.hasNext()) {
                    sb.append(UNION);
                }
            }
            return sb.toString();
        }
    }

    public static class Graph extends LinkedList<Pattern> implements
            Collection<Pattern>, Pattern {
        private static final long serialVersionUID = -1L;

        private static String filterOpen = "FILTER ( ".intern();

        private static String filterClose = " ) ".intern();

        public String filter;

        public Graph addGraph() {
            Graph g = new Graph();
            add(g);
            return g;
        }

        public Graph addTriplePattern(String s, String p, String o) {
            Triple triple = new Triple(s, p, o);
            add(triple);
            return this;
        }

        public Graph addTriplePattern(String s, String p, Literal o) {
            Triple triple = new Triple(s, p, literal(o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(String s, String p, URI o) {
            Triple triple = new Triple(s, p, uri(o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(String s, String p, Value o) {
            Triple triple =
                    new Triple(s, p, o instanceof URI ? uri((URI) o)
                            : literal((Literal) o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(String s, URI p, String o) {
            Triple triple = new Triple(s, uri(p), o);
            add(triple);
            return this;
        }

        public Graph addTriplePattern(String s, URI p, Literal o) {
            Triple triple = new Triple(s, uri(p), literal(o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(String s, URI p, URI o) {
            Triple triple = new Triple(s, uri(p), uri(o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(String s, URI p, Value o) {
            Triple triple =
                    new Triple(s, uri(p), o instanceof URI ? uri((URI) o)
                            : literal((Literal) o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(URI s, String p, String o) {
            Triple triple = new Triple(uri(s), p, o);
            add(triple);
            return this;
        }

        public Graph addTriplePattern(URI s, String p, Literal o) {
            Triple triple = new Triple(uri(s), p, literal(o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(URI s, String p, URI o) {
            Triple triple = new Triple(uri(s), p, uri(o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(URI s, String p, Value o) {
            Triple triple =
                    new Triple(uri(s), p, o instanceof URI ? uri((URI) o)
                            : literal((Literal) o));
            add(triple);
            return this;
        }

        public Graph addTriplePattern(URI s, URI p, String o) {
            Triple triple = new Triple(uri(s), uri(p), o);
            add(triple);
            return this;
        }

        public Graph setFilter(String filter) {
            this.filter = filter;
            return this;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (isEmpty()) {
                return sb.toString();
            }
            sb.append("{ ");
            if (filter != null) {
                sb.append(filterOpen).append(filter).append(filterClose);
            }
            Iterator<Pattern> it = iterator();
            while (it.hasNext()) {
                sb.append(it.next().toString());
                if (it.hasNext()) {
                    sb.append(' ');
                }
            }
            sb.append(" }");
            return sb.toString();
        }
    }

    public static class Triple implements Pattern {
        public String s, p, o;

        public static Collection<Character> valid =
                Arrays.asList(new Character[] { '?', '<', '\"' });

        public Triple(String s, String p, String o) {
            this.s = s;
            this.p = p;
            this.o = o;
            if (!valid.contains(s.charAt(0))) {
                throw new IllegalArgumentException(
                        "bad subject: must start with ?, <, or \"");
            }
            if (!valid.contains(p.charAt(0))) {
                throw new IllegalArgumentException(
                        "bad predicate: must start with ?, <, or \"");
            }
            if (!valid.contains(o.charAt(0))) {
                throw new IllegalArgumentException(
                        "bad object: must start with ?, <, or \"");
            }
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb
                    .append(s).append(' ').append(p).append(' ').append(o)
                    .append(' ').append('.');
            return sb.toString();
        }
    }
}
