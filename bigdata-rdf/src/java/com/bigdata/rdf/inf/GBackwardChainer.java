package com.bigdata.rdf.inf;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeSet;

import org.CognitiveWeb.generic.IGeneric;
import org.CognitiveWeb.sesame.sailimpl.generic.GRdfSchemaRepository;
import org.CognitiveWeb.sesame.sailimpl.generic.Profiler;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GBNode;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GGraph;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GResource;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GStatement;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GStatementIterator;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GURI;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GValue;
import org.CognitiveWeb.sesame.sailimpl.generic.model.GValueFactory;
import org.CognitiveWeb.sesame.sailimpl.generic.model.RDFPropertySchema;
import org.CognitiveWeb.sesame.sailimpl.generic.reasoner.GReasoner;
import org.CognitiveWeb.sesame.sailimpl.generic.reasoner.backward.core.MRPReasoner;
import org.CognitiveWeb.sesame.sailimpl.generic.reasoner.backward.core.SupportedStatement;
import org.CognitiveWeb.sesame.sailimpl.generic.reasoner.backward.core.rules.RdfsRuleModel;
import org.apache.log4j.Logger;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sesame.sail.SailChangedEvent;
import org.openrdf.sesame.sail.SailChangedListener;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.sesame.sail.util.EmptyStatementIterator;
import org.openrdf.vocabulary.OWL;

public class GBackwardChainer extends GReasoner implements SailChangedListener
{
    
    public static final Logger log = Logger.getLogger
        ( GBackwardChainer.class
          );
    
    
    private boolean inferStypeResource = false;
    
    private boolean inferOwlSameAs = false;
    
    
    public GBackwardChainer( GRdfSchemaRepository repo )
    {
        
        super( repo );
        
        repo.addListener( this );
        
    }
    
    public void setInferStypeResource( boolean inferStypeResource ) 
    {
    
        this.inferStypeResource = inferStypeResource;
        
    }
    
    public void setInferOwlSameAs( boolean inferOwlSameAs ) 
    {
    
        this.inferOwlSameAs = inferOwlSameAs;
        
    }
    
    public void sailChanged( SailChangedEvent e )
    {
     
        if ( e.statementsAdded() ) {
            
            // invalidate false goals
            
        }
        
        if ( e.statementsRemoved() ) {
            
            // invalidate proved goals
            
        }
        
    }
    
    public void setRuleModel( BackwardRuleModel ruleModel )
    {
        
    }
    
    /**
     * If true, reasoner will memoize false goals within the scope of a query.
     * 
     * @param b boolean flag
     */
    
    public void setMemoizeFalseGoals( boolean b )
    {
        
    }
    
    /**
     * If true, reasoner will memoize proved goals within the scope of a query.
     * 
     * @param b boolean flag
     */
    
    public void setMemoizeProvedGoals( boolean b )
    {
        
    }
    
    /**
     * If true, memoization will span multiple queries (i.e. the memo caches
     * will not be cleared after the query completes).
     * 
     * @param b boolean flag
     */
    
    public void setMemoizeAcrossQueries( boolean b )
    {
        
    }
    
    /**
     * If true, reasoner will persist proved goals in the KB.
     * 
     * @param b boolean flag
     * 
     * @see GReasoner#createInference(String, GStatement, GStatement, IGeneric, IGeneric, IGeneric)
     */
    
    public void setPersistProvedGoals( boolean b )
    {
        
    }
    
    public StatementIterator getStatements( Resource s, URI p, Value o )
    {

        // here is the KB to work with
        
        StatementIterator stmts = getGraph().getStatements( s, p, o );
        
        GStatementIterator gstmts = null;
        
        if ( stmts instanceof GStatementIterator ) {
            
            gstmts = (GStatementIterator) stmts;
            
        } else if ( stmts instanceof EmptyStatementIterator ) {
            
            gstmts = new GStatementIterator();
            
        } else {
            
            throw new RuntimeException( "strange statement iterator: " + stmts.getClass() );
            
        }

        computeInferences( s, p, o, gstmts );
        
        return gstmts;
        
    }
    
    protected void computeInferences
        ( Resource s, URI p, Value o,
          GStatementIterator stmts
          )
    {

        if ( inferStypeResource ) {
        
            inferStypeResource( s, p, o, stmts );
            
        }
        
        if ( inferOwlSameAs ) {
            
            inferOwlSameAs( s, p, o, stmts );
            
        }
        
        // inferOwlSameAs( s, p, o, gstmts );
/*
        get all explicit statements
        
        use rule model to prove all inferences
        
        if doing persistent memoization use super.createInference
        
        make sure to use:
            GGraph.lookup( IGeneric, IGeneric, IGeneric) : GStatement
            and
            GGraph.search( IGeneric, IGeneric, IGeneric) : GStatementIterator
        for optimized searching of the KB for rule bodies
        
        make sure you are using the NoProofsStrategy for truth maintenance
            - see ComparisonTest.java for an example
            - this way if you are persisting proved goals they will be 
              discarded upon statement removal (brute force truth maintenance)

        issues to keep in mind:
            - if keeping the memo caches around across queries, statement
              removal may invalidate certain proved goals and statement 
              addition may invalidate certain false goals
            - same goes for persistent proved goals (i.e. inferences): truth
              maintenance becomes an issue 

*/
    }
    
    private void inferStypeResource
        ( Resource s, URI p, Value o,
          GStatementIterator stmts
          )
    {
        
        GGraph graph = getGraph();
        
        GURI rdfType = (GURI) graph.lookup( URIImpl.RDF_TYPE, true );
        
        GURI rdfsResource = (GURI) graph.lookup( URIImpl.RDFS_RESOURCE, true );
        
        GResource gs = s != null ? (GResource) graph.lookup( s ) : null;
        
        GURI gp = p != null ? (GURI) graph.lookup( p ) : null;
        
        GValue go = o != null ? graph.lookup( o ) : null;

        if ( ( s != null && gs == null ) ||
             ( p != null && gp == null ) ||
             ( o != null && go == null ) ) {
            
            // can't know anything about a value not in our graph
            
            return;
            
        }
        
        if ( ( gp == null || 
               gp.asGeneric().equals( rdfType.asGeneric() ) ) &&
             ( go == null || 
               go.asGeneric().equals( rdfsResource.asGeneric() ) ) ) {
            
            if ( gs == null ) {
            
                // add a ( ? type Resource ) inference for every distinct
                // non-Literal value in the KB
                
                Iterator uris = graph.asGeneric().getLinkSet
                    ( RDFPropertySchema.URI_LINKSET 
                      ).iterator();
                
                while ( uris.hasNext() ) {
                    
                    IGeneric uri = (IGeneric) uris.next();
                    
                    // make sure the uri actually participates in at least one
                    // statement

                    if ( participatesInAStatement( uri ) ) {
                        
                        // log( "uri: " + uri.getString( RDFPropertySchema.URI ) );
                        
                        Statement stmt = graph.lookup
                            ( uri, 
                              rdfType.asGeneric(), 
                              rdfsResource.asGeneric() 
                              );
                        
                        if ( stmt == null ) {
                       
                            stmt = new TransientInference
                                ( (GURI) uri.asClass( GURI.class ),
                                  rdfType,
                                  rdfsResource
                                  );
                   
                            // log( "adding stmt: " + 
                            //      GStatement.toString( stmt, true ) );
                            
                            stmts.add( stmt );
                           
                       }
                        
                    }
                    
                }
                
                Iterator bnodes = graph.asGeneric().getLinkSet
                    ( RDFPropertySchema.BNODE_LINKSET 
                      ).iterator();
            
                while ( bnodes.hasNext() ) {
                    
                    IGeneric bnode = (IGeneric) bnodes.next();
                    
                    // make sure the uri actually participates in at least one
                    // statement

                    if ( participatesInAStatement( bnode ) ) {
                        
                        Statement stmt = graph.lookup
                            ( bnode, 
                              rdfType.asGeneric(), 
                              rdfsResource.asGeneric() 
                              );
                        
                        if ( stmt == null ) {
                       
                            stmt = new TransientInference
                                ( (GBNode) bnode.asClass( GBNode.class ),
                                  rdfType,
                                  rdfsResource
                                  );
                   
                            stmts.add( stmt );
                           
                        }

                    }
                    
                }
                
            } else {
               
                // add a ( s type Resource ) inference for the bound subject,
                // if it's not already in the result set
                
                Statement stmt = graph.lookup
                    ( gs.asGeneric(), 
                      rdfType.asGeneric(), 
                      rdfsResource.asGeneric() 
                      );
                
                if ( stmt == null ) {
               
                    stmt = new TransientInference
                        ( gs,
                          rdfType,
                          rdfsResource
                          );
           
                    stmts.add( stmt );
                   
                }
                
            }
            
        }
        
    }
    
    private boolean participatesInAStatement( IGeneric uri )
    {
        
        return uri.getLinkSet( RDFPropertySchema.SUBJECT ).size() > 0 ||
               uri.getLinkSet( RDFPropertySchema.PREDICATE ).size() > 0 ||
               uri.getLinkSet( RDFPropertySchema.OBJECT ).size() > 0;
        
    }
    
    /**
     * A couple of access paths are relevant here:
     * 
     * spo
     * sp?
     * s??
     * ?po
     * ??o
     * ???
     * 
     * 
     * @param s
     * @param p
     * @param o
     * @param stmts
     */
    
    private void inferOwlSameAs
        ( Resource s, URI p, Value o,
          GStatementIterator stmts
          )
    {
        
        GGraph graph = getGraph();
        
        GResource gs = s != null ? (GResource) graph.lookup( s ) : null;
        
        GURI gp = p != null ? (GURI) graph.lookup( p ) : null;
        
        GValue go = o != null ? graph.lookup( o ) : null;

        if ( ( s != null && gs == null ) ||
             ( p != null && gp == null ) ||
             ( o != null && go == null ) ) {
            
            // can't know anything about a value not in our graph
            
            return;
            
        }

        if ( gs != null && gp != null && go != null ) {
            
            accessSPO( gs, gp, go, stmts );
            
        } else if ( gs != null && gp != null && go == null ) {
            
            accessSP( gs, gp, stmts );
            
        } else if ( gs != null && gp == null && go != null ) {
            
            accessSO( gs, go, stmts );
            
        } else if ( gs == null && gp != null && go != null ) {
            
            accessPO( gp, go, stmts );
            
        } else if ( gs != null && gp == null && go == null ) {
            
            accessS( gs, stmts );
            
        } else if ( gs == null && gp != null && go == null ) {
            
            accessP( gp, stmts );
            
        } else if ( gs == null && gp == null && go != null ) {
            
            accessO( go, stmts );
            
        } else if ( gs == null && gp == null && go == null ) {
            
            accessAll( stmts );
            
        }
        
    }
    
    private void accessSPO
        ( GResource sSkin, GURI pSkin, GValue oSkin,
          GStatementIterator stmts )
    {
        
        IGeneric s = sSkin.asGeneric();
        
        IGeneric p = pSkin.asGeneric();
        
        IGeneric o = oSkin.asGeneric();
        
        GGraph graph = getGraph();
        
        // check to see if the statement exists explicitly
        
        if ( graph.lookup( s, p, o ) != null ) {
            
            return;
           
        }
        
        { // check to see if any statements exists replacing s with its sames

            Iterator sames = getSames( s ).iterator();
            
            while ( sames.hasNext() ) {
                
                IGeneric same = (IGeneric) sames.next();
                
                GStatement stmt = graph.lookup( same, p, o );
                
                if ( stmt != null ) {
                    
                    stmts.add( new TransientInference( sSkin, pSkin, oSkin ) );
                    
                    return;
                    
                }
                
            }
            
        }
        
        { // check to see if any statements exists replacing o with its sames
            
            Iterator sames = getSames( o ).iterator();
            
            while ( sames.hasNext() ) {
                
                IGeneric same = (IGeneric) sames.next();
                
                GStatement stmt = graph.lookup( s, p, same );
                
                if ( stmt != null ) {
                    
                    stmts.add( new TransientInference( sSkin, pSkin, oSkin ) );
                    
                    return;
                    
                }
                
            }

        }
        
    }
    
    private void accessS
        ( GResource sSkin,
          GStatementIterator stmts )
    {
        
        accessSP( sSkin, null, stmts );
        
    }
    
    private void accessSO
        ( GResource sSkin, GValue oSkin,
          GStatementIterator stmts )
    {
        
        throw new RuntimeException( "not yet implemented" );
        
    }
    
    private void accessPO
        ( GURI pSkin, GValue oSkin,
          GStatementIterator stmts )
    {
        
        GGraph graph = getGraph();
        
        IGeneric p = pSkin.asGeneric();
        
        IGeneric o = oSkin.asGeneric();
        
        Set explicit = new TreeSet(); 
        
        Set inferred = new TreeSet();
 
        
        
        Set all = new TreeSet();

        { // get all the subjects that explicitly have this property
        
            StatementIterator it = graph.search( null, p, o );
            
            while ( it.hasNext() ) {
                
                GStatement stmt = (GStatement) it.next();
                
                SPTuple tuple = new SPTuple( stmt );
                
                explicit.add( tuple );
                
                all.add( tuple );
                
            }
            
            it.close();
            
        }
        
        { // get all the objects that are the same as the supplied object and get all subjects that explicitly have those objects
        
            Iterator sames = getSames( o ).iterator();
            
            while ( sames.hasNext() ) {
                
                IGeneric same = (IGeneric) sames.next();
                
                StatementIterator it = graph.search( null, p, same );
                
                while ( it.hasNext() ) {
                    
                    GStatement stmt = (GStatement) it.next();
                    
                    SPTuple tuple = new SPTuple( stmt );
                    
                    if ( !explicit.contains( tuple ) ) {
                        
                        inferred.add( tuple );
                        
                        all.add( tuple );
                        
                    }
                    
                }
                
            }
            
        }
        
        { // now get all subjects that are the same as ones that have been retrieved
            
            Iterator it = all.iterator();
            
            while ( it.hasNext() ) {
                
                SPTuple tuple = (SPTuple) it.next();
                
                Iterator sames = getSames( tuple.s ).iterator();
                
                while ( sames.hasNext() ) {
                    
                    IGeneric same = (IGeneric) sames.next();
                    
                    GResource sameSkin = null;
                    
                    if ( tuple.sSkin instanceof GBNode ) {
                        
                        sameSkin = (GBNode) same.asClass( GBNode.class );
                        
                    } else {
                        
                        sameSkin = (GURI) same.asClass( GURI.class );
                        
                    }
                    
                    SPTuple sameTuple = new SPTuple( sameSkin, tuple.pSkin );
                    
                    if ( !explicit.contains( sameTuple ) ) {
                        
                        inferred.add( sameTuple );
                        
                    }
                    
                }
                
            }
            
        }

        // add a transient inference for each subject, p, o

        Iterator it = inferred.iterator();
        
        while ( it.hasNext() ) {
            
            SPTuple tuple = (SPTuple) it.next();
            
            stmts.add
                ( new TransientInference( tuple.sSkin, tuple.pSkin, oSkin ) 
                  );
            
        }
        
    }
    
    private void accessSP
        ( GResource sSkin, GURI pSkin,
          GStatementIterator stmts )
    {
        
        IGeneric s = sSkin.asGeneric();
        
        IGeneric p = pSkin != null ? pSkin.asGeneric() : null;
        
        GGraph graph = getGraph();
        
        Set explicit = new TreeSet();
        
        Set inferred = new TreeSet();
        
        Set all = new TreeSet();
        
        { // get all of S's properties
            
            StatementIterator it = graph.search( s, p, null );
            
            while ( it.hasNext() ) {
                
                GStatement stmt = (GStatement) it.next();
                
                POTuple tuple = new POTuple( stmt );
                
                explicit.add( tuple );
                
                all.add( tuple );
                
            }
            
            it.close();
        
        }

        { // get all of S's sameAs's and those properties

            Iterator sames = getSames( s.asGeneric() ).iterator();
            
            while ( sames.hasNext() ) {
                
                IGeneric same = (IGeneric) sames.next();
                
                StatementIterator it = graph.search( same, p, null );
                
                while ( it.hasNext() ) {
                    
                    GStatement stmt = (GStatement) it.next();
                    
                    POTuple tuple = new POTuple( stmt );
                    
                    if ( !explicit.contains( tuple ) ) {
                    
                        inferred.add( tuple );
                        
                        all.add( tuple );
                        
                    }
                    
                }
                
                it.close();
                
            }
            
        }
        
        // now that we have all properties, get all of the properties' sameAs's
        
        {
            
            Iterator it = all.iterator();
            
            while ( it.hasNext() ) {
                
                POTuple prop = (POTuple) it.next();

                if ( prop.oSkin instanceof Literal ) {
                    
                    continue;
                    
                }
                
                Iterator sames = getSames( prop.o ).iterator();
                
                while ( sames.hasNext() ) {
                    
                    IGeneric same = (IGeneric) sames.next();

                    GValue sameSkin = null;
                    
                    if ( prop.oSkin instanceof GBNode ) {
                        
                        sameSkin = (GBNode) same.asClass( GBNode.class );
                        
                    } else {
                        
                        sameSkin = (GURI) same.asClass( GURI.class );
                        
                    }
                    
                    POTuple sameTuple = new POTuple( prop.pSkin, sameSkin );
                    
                    if ( !explicit.contains( sameTuple ) ) {
                        
                        inferred.add( sameTuple );
                        
                    }
                    
                }
                
            }
            
        }
        
        Iterator it = inferred.iterator();
        
        while ( it.hasNext() ) {
            
            POTuple prop = (POTuple) it.next();
            
            stmts.add
                ( new TransientInference( sSkin, prop.pSkin, prop.oSkin ) 
                  );
            
        }
        
    }
    
    private void accessP
        ( GURI pSkin,
          GStatementIterator stmts )
    {
        
        throw new RuntimeException( "not yet implemented" );
        
    }
    
    private void accessO
        ( GValue oSkin,
          GStatementIterator stmts )
    {
        
        accessPO( null, oSkin, stmts );
        
    }
    
    private void accessAll( GStatementIterator stmts )
    {
        
        throw new RuntimeException( "not yet implemented" );
        
    }
        
    private class POTuple implements Comparable
    {
        
        private GURI pSkin;
        
        private GValue oSkin;
        
        private IGeneric p, o;
        
        private int hashCode;
        
        private String toString;
        
        public POTuple( GStatement stmt )
        {
            
            this( stmt.getGPredicate(), stmt.getGObject() );
            
        }
            
        public POTuple( GURI pSkin, GValue oSkin )
        {
            
            this.pSkin = pSkin;
            
            this.oSkin = oSkin;
            
            this.p = pSkin.asGeneric();
            
            this.o = oSkin.asGeneric();
           
            this.hashCode = ( p.identity() + o.identity() ).hashCode();
            
            this.toString = "(" + p.identity() + ", " + o.identity() + ")";
            
        }
        
        public int hashCode()
        {
            
            return hashCode;
            
        }
        
        public boolean equals( Object obj )
        {
            
            if ( obj == this ) {
                
                return true;
                
            }
            
            return obj instanceof POTuple &&
                   ( (POTuple) obj ).p == this.p &&
                   ( (POTuple) obj ).o == this.o;
            
        }
        
        public String toString()
        {
            
            return toString;
            
        }
     
        public int compareTo( Object obj )
        {
        
            if ( obj == this ) {
                
                return 0;
                
            }
            
            return obj.toString().compareTo( toString );
            
        }
        
    }
    
    private class SPTuple implements Comparable
    {
        
        private GResource sSkin;
        
        private GURI pSkin;
        
        private IGeneric s, p;
        
        private int hashCode;
        
        private String toString;
        
        public SPTuple( GStatement stmt )
        {
            
            this( stmt.getGSubject(), stmt.getGPredicate() );
            
        }
            
        public SPTuple( GResource sSkin, GURI pSkin )
        {
         
            this.sSkin = sSkin;
            
            this.pSkin = pSkin;
            
            this.s = sSkin.asGeneric();
            
            this.p = pSkin.asGeneric();
           
            this.hashCode = ( s.identity() + p.identity() ).hashCode();
            
            this.toString = "(" + s.identity() + ", " + p.identity() + ")";
            
        }
        
        public int hashCode()
        {
            
            return hashCode;
            
        }
        
        public boolean equals( Object obj )
        {
            
            if ( obj == this ) {
                
                return true;
                
            }
            
            return obj instanceof SPTuple &&
                   ( (SPTuple) obj ).s == this.s &&
                   ( (SPTuple) obj ).p == this.p;
            
        }
        
        public String toString()
        {
            
            return toString;
            
        }
     
        public int compareTo( Object obj )
        {
        
            if ( obj == this ) {
                
                return 0;
                
            }
            
            return obj.toString().compareTo( toString );
            
        }
        
    }
    
    private Set getSames( IGeneric g )
    {
        
        Profiler.enterBlock( "getSames" );
        
        Set sames = new HashSet();
        
        sames.add( g );
        
        getSames( g, sames );
        
        sames.remove( g );
        
        Profiler.exitBlock( "getSames" );
        
        return sames;
        
    }
    
    private void getSames( IGeneric g, Set sames )
    {
        
        GGraph graph = getGraph();

        IGeneric owlSameAs = graph.lookupGPOforURI( OWL.SAMEAS );
        
        StatementIterator stmts = graph.search( g, owlSameAs, null );
        
        while ( stmts.hasNext() ) {
            
            IGeneric same = 
                ( (GStatement) stmts.next() ).getGObject().asGeneric();
            
            if ( ! sames.contains( same ) ) {
                
                sames.add( same );
                
                getSames( same, sames );
                
            }
            
        }
        
        stmts.close();
        
        stmts = graph.search( null, owlSameAs, g );
        
        while ( stmts.hasNext() ) {
            
            IGeneric same = 
                ( (GStatement) stmts.next() ).getGSubject().asGeneric();
            
            if ( ! sames.contains( same ) ) {
                
                sames.add( same );
                
                getSames( same, sames );
                
            }
            
        }
        
        stmts.close();
        
    }
    
    /**
     * Returns a persistent 
     * @param stmt
     * @return
     */
    public boolean prove( GStatement stmt )
    {
        
        MRPReasoner reasoner = new MRPReasoner( new RdfsRuleModel() );
        
        Statement s = reasoner.prove( repo, stmt );
        
        if ( s instanceof SupportedStatement ) {
        
            createProofs( (SupportedStatement) s );
            
        }
        
        return s != null;
        
    }
    
    private void createProofs( SupportedStatement stmt )
    {

        GGraph graph = repo.getGraph();
        
        GValueFactory gvf = graph.getGValueFactory();
        
        Collection supportsToAdd = new LinkedList();
        
        for ( Iterator it = stmt.getSupports(); it.hasNext(); ) {
            
            Statement support = (Statement) it.next();
            
            if ( support instanceof SupportedStatement ) {
            
                createProofs( stmt );
                
                it.remove();

                GStatement gsupport = gvf.createInference( support );
                
                supportsToAdd.add( gsupport );
                
            }
                
        }
        
        for ( Iterator it = supportsToAdd.iterator(); it.hasNext(); ) {
            
            stmt.addSupport( (GStatement) it.next() );
            
        }
        
        GStatement support1 = (GStatement) stmt.getSupport1();
        
        GStatement support2 = (GStatement) stmt.getSupport2();
        
        GStatement consequence = graph.lookup( stmt );
        
        if ( consequence == null ) {
            
            consequence = gvf.createInference( stmt );
            
        }
        
        gvf.createProof
            ( "",
              support1,
              support2,
              consequence
              );
        
    }
    
    private static void log( String s )
    {
        
        log.debug( s );
        
        System.out.println( s );
        
    }

}
