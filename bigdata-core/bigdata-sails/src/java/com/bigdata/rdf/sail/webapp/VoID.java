/**

Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
/*
 * Created on Jul 24, 2012
 */
package com.bigdata.rdf.sail.webapp;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.BNode;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.NotMaterializedException;
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.sail.webapp.client.ConnectOptions;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPORelation;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataValueIterator;
import com.bigdata.rdf.store.BigdataValueIteratorImpl;
import com.bigdata.rdf.vocab.decls.DCTermsVocabularyDecl;
import com.bigdata.rdf.vocab.decls.VoidVocabularyDecl;
import com.bigdata.striterator.IChunkedIterator;

/**
 * Helper class for VoID descriptions.
 * 
 * @see <a href="http://www.w3.org/TR/void/" > Describing Linked Datasets with
 *      the VoID Vocabulary </a>
 *      
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 */
public class VoID {

    /**
     * The graph in which the service description is accumulated (from the
     * constructor).
     */
    private final Graph g;
    
    /**
     * The KB instance that is being described (from the constructor).
     */
    private final AbstractTripleStore tripleStore;
    
    /**
     * The service end point(s) (from the constructor).
     */
    private final String[] serviceURI;

    /**
     * The value factory used to create values for the service description graph
     * {@link #g}.
     */
    private final ValueFactory f;
    
//    /**
//     * The resource which models the service.
//     */
//    protected final BNode aService;

    /**
     * The resource which models the data set.
     */
    private final Resource aDataset;

    /**
     * The resource which models the default graph in the data set.
     */
    private final BNode aDefaultGraph;

    /**
     * 
     * @param g
     *            Where to assemble the description.
     * @param tripleStore
     *            The KB instance to be described.
     * @param serviceURI
     *            The SPARQL service end point.
     * @param aDefaultDataset
     *            The data set identifier that will be used on the description
     *            (the bigdata namespace of the dataset is obtained from the
     *            <i>tripleStore</i>).
     */
    public VoID(final Graph g, final AbstractTripleStore tripleStore,
            final String[] serviceURI, final Resource aDataset) {

        if (g == null)
            throw new IllegalArgumentException();

        if (tripleStore == null)
            throw new IllegalArgumentException();
        
        if (serviceURI == null)
            throw new IllegalArgumentException();

        if (serviceURI.length == 0)
            throw new IllegalArgumentException();

        for (String s : serviceURI)
            if (s == null)
                throw new IllegalArgumentException();
        
        if (aDataset == null)
            throw new IllegalArgumentException();

        this.g = g;
        
        this.tripleStore = tripleStore;
        
        this.serviceURI = serviceURI;
        
        this.f = g.getValueFactory();
        
        this.aDataset = aDataset;
    
        this.aDefaultGraph = f.createBNode("defaultGraph");
        
    }
    
    /**
     * Describe the default data set (the one identified by the namespace
     * associated with the {@link AbstractTripleStore}.
     * 
     * @param describeStatistics
     *            When <code>true</code>, the VoID description will include the
     *            {@link VoidVocabularyDecl#vocabulary} declarations, the
     *            property partition statistics, and the class partition
     *            statistics.
     * @param describeNamedGraphs
     *            When <code>true</code>, each named graph will also be
     *            described in in the same level of detail as the default graph.
     *            Otherwise only the default graph will be described.
     */
    public void describeDataSet(final boolean describeStatistics,
            final boolean describeNamedGraphs) {

        final String namespace = tripleStore.getNamespace();
        
        // This is a VoID data set.
        g.add(aDataset, RDF.TYPE, VoidVocabularyDecl.Dataset);

        // The namespace is used as a title for the data set.
        g.add(aDataset, DCTermsVocabularyDecl.title,
                f.createLiteral(namespace));

        // Also present the namespace in an unambiguous manner.
        g.add(aDataset, SD.KB_NAMESPACE, f.createLiteral(namespace));

        /**
         * Service end point for this namespace.
         * 
         * @see <a href="https://sourceforge.net/apps/trac/bigdata/ticket/689" >
         *      Missing URL encoding in RemoteRepositoryManager </a>
         */
        for (String uri : serviceURI) {
            g.add(aDataset,
                    VoidVocabularyDecl.sparqlEndpoint,
                    f.createURI(uri + "/" + ConnectOptions.urlEncode(namespace)
                            + "/sparql"));
        }

        // any URI is considered to be an entity.
        g.add(aDataset, VoidVocabularyDecl.uriRegexPattern,
                f.createLiteral("^.*"));

        if(!describeStatistics) {
        
            // No statistics.
            return;
            
        }
        
        // Frequency count of the predicates in the default graph.
        final IVCount[] predicatePartitionCounts = predicateUsage(
                tripleStore);

        // Frequency count of the classes in the default graph.
        final IVCount[] classPartitionCounts = classUsage(tripleStore);

        // Describe vocabularies based on the predicate partitions.
        describeVocabularies(predicatePartitionCounts);
        
        // defaultGraph description.
        {

            // Default graph in the default data set.
            g.add(aDataset, SD.defaultGraph, aDefaultGraph);
            
            // Describe the default graph using statistics.
            describeGraph(aDefaultGraph, predicatePartitionCounts,
                    classPartitionCounts);

        } // end defaultGraph

        // sb.append("termCount\t = " + tripleStore.getTermCount() + "\n");
        //
        // sb.append("uriCount\t = " + tripleStore.getURICount() + "\n");
        //
        // sb.append("literalCount\t = " + tripleStore.getLiteralCount() +
        // "\n");
        //
        // /*
        // * Note: The blank node count is only available when using the told
        // * bnodes mode.
        // */
        // sb
        // .append("bnodeCount\t = "
        // + (tripleStore.getLexiconRelation()
        // .isStoreBlankNodes() ? ""
        // + tripleStore.getBNodeCount() : "N/A")
        // + "\n");

        /*
         * Report for each named graph.
         */
        if (describeNamedGraphs && tripleStore.isQuads()) {

            final SPORelation r = tripleStore.getSPORelation();

            // the index to use for distinct term scan.
            final SPOKeyOrder keyOrder = SPOKeyOrder.CSPO;

            // visit distinct IVs for context position on that index.
            @SuppressWarnings("rawtypes")
            final IChunkedIterator<IV> itr = r.distinctTermScan(keyOrder);

            // resolve IVs to terms efficiently during iteration.
            final BigdataValueIterator itr2 = new BigdataValueIteratorImpl(
                    tripleStore/* resolveTerms */, itr);

            try {

                while (itr2.hasNext()) {

                    /*
                     * Describe this named graph.
                     * 
                     * Note: This is using the predicate and class partition
                     * statistics from the default graph (RDF merge) to identify
                     * the set of all possible predicates and classes within
                     * each named graph. It then tests each predicate and class
                     * partition against the named graph and ignores those which
                     * are not present in a given named graph. This is being
                     * done because we do not have a CPxx index.
                     */

                    final BigdataResource graph = (BigdataResource) itr2.next();

                    final IVCount[] predicatePartitionCounts2 = predicateUsage(
                            tripleStore, graph.getIV(),
                            predicatePartitionCounts);

                    final IVCount[] classPartitionCounts2 = classUsage(
                            tripleStore, graph.getIV(), classPartitionCounts);

                    final BNode aNamedGraph = f.createBNode();

                    // Named graph in the default data set.
                    g.add(aDataset, SD.namedGraph, aNamedGraph);

                    // The name of that named graph.
                    g.add(aNamedGraph, SD.name, graph);

                    // Describe the named graph.
                    describeGraph(aNamedGraph, predicatePartitionCounts2,
                            classPartitionCounts2);

                }
                
            } finally {

                itr2.close();

            }

        }

    }

    /**
     * Describe the vocabularies which are in use in the KB based on the
     * predicate partition statistics.
     * 
     * @param predicateParitionCounts
     *            The predicate partition statistics.
     */
    protected void describeVocabularies(final IVCount[] predicatePartitionCounts) {

        // Find the distinct vocabularies in use.
        final Set<String> namespaces = new LinkedHashSet<String>();
        {

            // property partitions.
            for (IVCount tmp : predicatePartitionCounts) {

                final URI p = (URI) tmp.getValue();

                String namespace = p.getNamespace();

                if (namespace.endsWith("#")) {

                    // Strip trailing '#' per VoID specification.
                    namespace = namespace.substring(0,
                            namespace.length() - 1);

                }

                namespaces.add(namespace);

            }

        }

        // Sort into dictionary order.
        final String[] a = namespaces.toArray(new String[namespaces.size()]);
        
        Arrays.sort(a);

        for (String namespace : a) {

            g.add(aDataset, VoidVocabularyDecl.vocabulary,
                    f.createURI(namespace));

        }

    }
    
    /**
     * Describe a named or default graph.
     * 
     * @param graph
     *            The named graph.
     * @param predicatePartitionCounts
     *            The predicate partition statistics for that graph.
     * @param classPartitionCounts
     *            The class partition statistics for that graph.
     */
    protected void describeGraph(final Resource graph,
            final IVCount[] predicatePartitionCounts,
            final IVCount[] classPartitionCounts) {

        // The graph is a Graph.
        g.add(graph, RDF.TYPE, SD.Graph);

        // #of triples in the default graph
        g.add(graph, VoidVocabularyDecl.triples,
                f.createLiteral(tripleStore.getStatementCount()));

        // #of entities in the default graph.
        g.add(graph, VoidVocabularyDecl.entities,
                f.createLiteral(tripleStore.getURICount()));

        // #of distinct predicates in the default graph.
        g.add(graph, VoidVocabularyDecl.properties,
                f.createLiteral(predicatePartitionCounts.length));

        // #of distinct classes in the default graph.
        g.add(graph, VoidVocabularyDecl.classes,
                f.createLiteral(classPartitionCounts.length));

        // property partition statistics.
        for (IVCount tmp : predicatePartitionCounts) {

            final BNode propertyPartition = f.createBNode();

            final URI p = (URI) tmp.getValue();

            g.add(graph, VoidVocabularyDecl.propertyPartition,
                    propertyPartition);

            g.add(propertyPartition, VoidVocabularyDecl.property, p);

            g.add(propertyPartition, VoidVocabularyDecl.triples,
                    f.createLiteral(tmp.count));

        }

        // class partition statistics.
        {

            // per class partition statistics.
            for (IVCount tmp : classPartitionCounts) {

                final BNode classPartition = f.createBNode();

                final BigdataValue cls = tmp.getValue();

                g.add(graph, VoidVocabularyDecl.classPartition,
                        classPartition);

                g.add(classPartition, VoidVocabularyDecl.class_, cls);

                g.add(classPartition, VoidVocabularyDecl.triples,
                        f.createLiteral(tmp.count));

            }

        } // end class partition statistics.

    }

    /**
     * An {@link IV} and a counter for that {@link IV}.
     */
    protected static class IVCount implements Comparable<IVCount> {

        public final IV<?, ?> iv;

        public final long count;

        private BigdataValue val;
        
        /**
         * Return the associated {@link BigdataValue}.
         * <p>
         * Note: A resolution set is necessary if you want to attach the
         * {@link BigdataValue} to the {@link IV}.
         * 
         * @throws NotMaterializedException
         */
        public BigdataValue getValue() {

            if(val == null)
                throw new NotMaterializedException(iv.toString());
            
            return val;
            
        }
        
        public void setValue(final BigdataValue val) {

            if (val == null)
                throw new IllegalArgumentException();
            
            if (this.val != null && !this.val.equals(val))
                throw new IllegalArgumentException();
            
            this.val = val;

        }
        
        public IVCount(final IV<?,?> iv, final long count) {

            if (iv == null)
                throw new IllegalArgumentException();
            
            this.iv = iv;
            
            this.count = count;
            
        }

        /**
         * Place into order by descending count.
         */
        @Override
        public int compareTo(IVCount arg0) {

            if (count < arg0.count)
                return 1;

            if (count > arg0.count)
                return -1;

            return 0;
            
        }
        
    }

    /**
     * Return an array of the distinct predicates in the KB ordered by their
     * descending frequency of use. The {@link IV}s in the returned array will
     * have been resolved to the corresponding {@link BigdataURI}s which can be
     * accessed using {@link IV#getValue()}.
     * 
     * @param kb
     *            The KB instance.
     */
    protected static IVCount[] predicateUsage(final AbstractTripleStore kb) {

        final SPORelation r = kb.getSPORelation();
        
        if (r.oneAccessPath) {

            // The necessary index (POS or POCS) does not exist.
            throw new UnsupportedOperationException();

        }

        final boolean quads = kb.isQuads();

        // the index to use for distinct predicate scan.
        final SPOKeyOrder keyOrder = quads ? SPOKeyOrder.POCS : SPOKeyOrder.POS;

        // visit distinct term identifiers for predicate position on that index.
        @SuppressWarnings("rawtypes")
        final IChunkedIterator<IV> itr = r.distinctTermScan(keyOrder);

        // resolve term identifiers to terms efficiently during iteration.
        final BigdataValueIterator itr2 = new BigdataValueIteratorImpl(
                kb/* resolveTerms */, itr);

        try {

            final Set<IV<?,?>> ivs = new LinkedHashSet<IV<?,?>>();

            final Map<IV<?, ?>, IVCount> counts = new LinkedHashMap<IV<?, ?>, IVCount>();

            while (itr2.hasNext()) {

                final BigdataValue term = itr2.next();

                final IV<?,?> iv = term.getIV();

                final long n = r.getAccessPath(null, iv, null, null)
                        .rangeCount(false/* exact */);

                ivs.add(iv);
                
                counts.put(iv, new IVCount(iv, n));

            }

            // Batch resolve IVs to Values
            final Map<IV<?, ?>, BigdataValue> x = kb.getLexiconRelation()
                    .getTerms(ivs);

            for (Map.Entry<IV<?, ?>, BigdataValue> e : x.entrySet()) {

                final IVCount count = counts.get(e.getKey());

                count.setValue(e.getValue());

            }

            final IVCount[] a = counts.values().toArray(
                    new IVCount[counts.size()]);

            // Order by descending count.
            Arrays.sort(a);

            return a;

        } finally {

            itr2.close();

        }

    }

    /**
     * Return the predicate partition statistics for the named graph.
     * 
     * @param kb
     *            The KB instance.
     * @param civ
     *            The {@link IV} of a named graph (required).
     * 
     * @return The predicate partition statistics for that named graph. Only
     *         predicate partitions which are non-empty are returned.
     */
    protected static IVCount[] predicateUsage(final AbstractTripleStore kb,
            final IV<?, ?> civ, final IVCount[] predicatePartitionCounts) {
        
        final SPORelation r = kb.getSPORelation();

        final boolean quads = kb.isQuads();
        
        if (!quads) {
            
            // Named graph only valid in quads mode.
            throw new IllegalArgumentException();
        
        }

        // The non-zero counts.
        final List<IVCount> counts = new LinkedList<IVCount>();

        // Check the known non-empty predicate partitions.
        for(IVCount in : predicatePartitionCounts){

            final long n = r.getAccessPath(null, in.iv, null, civ).rangeCount(
                    false/* exact */);

            if (n == 0)
                continue;

            final IVCount out = new IVCount(in.iv, n);

            out.setValue(in.getValue());
            
            counts.add(out);

        }
        

        final IVCount[] a = counts.toArray(new IVCount[counts.size()]);

        // Order by descending count.
        Arrays.sort(a);

        return a;

    }
    
    /**
     * Return an efficient statistical summary for the class partitions. The
     * SPARQL query for this is
     * 
     * <pre>
     * SELECT  ?class (COUNT(?s) AS ?count ) { ?s a ?class } GROUP BY ?class ORDER BY ?count
     * </pre>
     * 
     * However, it is much efficient to scan POS for
     * 
     * <pre>
     * rdf:type ?o ?s
     * </pre>
     * 
     * and report the range count of
     * 
     * <pre>
     * rdf:type ?o ?s
     * </pre>
     * 
     * for each distinct value of <code>?o</code>.
     * 
     * @param kb
     *            The KB instance.
     *            
     * @return The class usage statistics.
     */
    protected static IVCount[] classUsage(final AbstractTripleStore kb) {

        final SPORelation r = kb.getSPORelation();

        if (r.oneAccessPath) {

            // The necessary index (POS or POCS) does not exist.
            throw new UnsupportedOperationException();

        }

        final boolean quads = kb.isQuads();

        final SPOKeyOrder keyOrder = quads ? SPOKeyOrder.POCS : SPOKeyOrder.POS;

        // Resolve IV for rdf:type
        final BigdataURI rdfType = kb.getValueFactory().asValue(RDF.TYPE);

        kb.getLexiconRelation().addTerms(new BigdataValue[] { rdfType },
                1/* numTerms */, true/* readOnly */);

        if (rdfType.getIV() == null) {

            // No rdf:type assertions since rdf:type is unknown term.
            return new IVCount[0];

        }
        
        // visit distinct term identifiers for the rdf:type predicate.
        @SuppressWarnings("rawtypes")
        final IChunkedIterator<IV> itr = r.distinctMultiTermScan(keyOrder,
                new IV[] { rdfType.getIV() }/* knownTerms */);

        // resolve term identifiers to terms efficiently during iteration.
        final BigdataValueIterator itr2 = new BigdataValueIteratorImpl(
                kb/* resolveTerms */, itr);

        try {

            final Set<IV<?,?>> ivs = new LinkedHashSet<IV<?,?>>();

            final Map<IV<?, ?>, IVCount> counts = new LinkedHashMap<IV<?, ?>, IVCount>();

            while (itr2.hasNext()) {

                final BigdataValue term = itr2.next();

                final IV<?,?> iv = term.getIV();

                final long n = r.getAccessPath(null, rdfType.getIV()/* p */,
                        iv/* o */, null).rangeCount(false/* exact */);

                ivs.add(iv);
                
                counts.put(iv, new IVCount(iv, n));

            }

            // Batch resolve IVs to Values
            final Map<IV<?, ?>, BigdataValue> x = kb.getLexiconRelation()
                    .getTerms(ivs);

            for (Map.Entry<IV<?, ?>, BigdataValue> e : x.entrySet()) {

                final IVCount count = counts.get(e.getKey());

                count.setValue(e.getValue());

            }

            final IVCount[] a = counts.values().toArray(
                    new IVCount[counts.size()]);

            // Order by descending count.
            Arrays.sort(a);

            return a;

        } finally {

            itr2.close();

        }

    }

    /**
     * Return the class partition statistics for the named graph.
     * 
     * @param kb
     *            The KB instance.
     * @param civ
     *            The {@link IV} of a named graph (required).
     * 
     * @return The class partition statistics for that named graph. Only class
     *         partitions which are non-empty are returned.
     */
    protected static IVCount[] classUsage(final AbstractTripleStore kb,
            final IV<?, ?> civ, final IVCount[] classPartitionCounts) {

        final SPORelation r = kb.getSPORelation();

        final boolean quads = kb.isQuads();

        if (!quads) {

            // Named graph only valid in quads mode.
            throw new IllegalArgumentException();

        }

        // Resolve IV for rdf:type
        final BigdataURI rdfType = kb.getValueFactory().asValue(RDF.TYPE);

        kb.getLexiconRelation().addTerms(new BigdataValue[] { rdfType },
                1/* numTerms */, true/* readOnly */);

        if (rdfType.getIV() == null) {

            // No rdf:type assertions since rdf:type is unknown term.
            return new IVCount[0];

        }

        // The non-zero counts.
        final List<IVCount> counts = new LinkedList<IVCount>();

        // Check the known non-empty predicate partitions.
        for (IVCount in : classPartitionCounts) {

            final long n = r.getAccessPath(null, rdfType.getIV()/* p */,
                    in.iv/* o */, civ).rangeCount(false/* exact */);

            if (n == 0)
                continue;
            
            final IVCount out = new IVCount(in.iv, n);

            out.setValue(in.getValue());

            counts.add(out);

        }

        final IVCount[] a = counts.toArray(new IVCount[counts.size()]);

        // Order by descending count.
        Arrays.sort(a);

        return a;

    }

}
