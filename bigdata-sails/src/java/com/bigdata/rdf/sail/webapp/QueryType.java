package com.bigdata.rdf.sail.webapp;

import java.util.Arrays;

/**
 * Helper class to figure out the type of a query.
 */
public enum QueryType {

    ASK(0), DESCRIBE(1), CONSTRUCT(2), SELECT(3);

    private final int order;

    private QueryType(final int order) {
        this.order = order;
    }

    private static QueryType getQueryType(final int order) {
        switch (order) {
        case 0:
            return ASK;
        case 1:
            return DESCRIBE;
        case 2:
            return CONSTRUCT;
        case 3:
            return SELECT;
        default:
            throw new IllegalArgumentException("order=" + order);
        }
    }

    /**
     * Used to note the offset at which a keyword was found.
     */
    static private class P implements Comparable<QueryType.P> {

        final int offset;

        final QueryType queryType;

        public P(final int offset, final QueryType queryType) {
            this.offset = offset;
            this.queryType = queryType;
        }

        /** Sort into descending offset. */
        public int compareTo(final QueryType.P o) {
            return o.offset - offset;
        }
    }

    /**
     * Hack returns the query type based on the first occurrence of the
     * keyword for any known query type in the query.
     * 
     * @param queryStr
     *            The query.
     * 
     * @return The query type.
     */
    static public QueryType fromQuery(final String queryStr) {

        // force all to lower case.
        final String s = queryStr.toUpperCase();

        final int ntypes = QueryType.values().length;

        final QueryType.P[] p = new QueryType.P[ntypes];

        int nmatch = 0;
        for (int i = 0; i < ntypes; i++) {

            final QueryType queryType = getQueryType(i);

            final int offset = s.indexOf(queryType.toString());

            if (offset == -1)
                continue;

            p[nmatch++] = new P(offset, queryType);

        }

        if (nmatch == 0) {

            throw new RuntimeException(
                    "Could not determine the query type: " + queryStr);

        }

        Arrays.sort(p, 0/* fromIndex */, nmatch/* toIndex */);

        final QueryType.P tmp = p[0];

        // System.out.println("QueryType: offset=" + tmp.offset + ", type="
        // + tmp.queryType);

        return tmp.queryType;

    }

}