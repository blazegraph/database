/*
 * Copyright SYSTAP, LLC 2006-2007.  All rights reserved.
 * 
 * Contact:
 *      SYSTAP, LLC
 *      4501 Tower Road
 *      Greensboro, NC 27410
 *      phone: +1 202 462 9888
 *      email: licenses@bigdata.com
 *
 *      http://www.systap.com/
 *      http://www.bigdata.com/
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/*
 * Created on Oct 25, 2007
 */

package com.bigdata.rdf.sail;

import java.util.Map;

import org.CognitiveWeb.util.PropertyUtil;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.sesame.sail.LiteralIterator;
import org.openrdf.sesame.sail.RdfSchemaRepository;
import org.openrdf.sesame.sail.SailInitializationException;
import org.openrdf.sesame.sail.StatementIterator;
import org.openrdf.vocabulary.RDF;
import org.openrdf.vocabulary.RDFS;

import com.bigdata.rdf.spo.ExplicitSPOFilter;
import com.bigdata.rdf.store.SesameStatementIterator;

/**
 * Implementation of {@link RdfSchemaRepository}.
 * 
 * @author <a href="mailto:thompsonbry@users.sourceforge.net">Bryan Thompson</a>
 * @version $Id$
 */
public class BigdataRdfSchemaRepository extends BigdataRdfRepository implements RdfSchemaRepository {

    /**
     * 
     */
    public BigdataRdfSchemaRepository() {

        super();
        
    }

    public void initialize(Map configParams) throws SailInitializationException {

        properties = PropertyUtil.flatCopy(PropertyUtil.convert(configParams));

        /*
         * This SAIL always enables truth maintenance.
         */
        
        properties.setProperty(Options.TRUTH_MAINTENANCE, "true");
        
        super.initialize( properties );
        
    }
    
    // Gets all explicitly added statements with a specific subject,
    // predicate and/or object.
    public StatementIterator getExplicitStatements(Resource s, URI p,
            Value o) {

        return new SesameStatementIterator(database, database.getAccessPath(s,
                p, o).iterator(ExplicitSPOFilter.INSTANCE));
        
    }


    // Checks if an explicitly added statement with a specific
    // subject, predicate and/or object is present in the repository.
    public boolean hasExplicitStatement
        ( Resource s,
          URI p,
          Value o
          )
    {

        return getExplicitStatements
            ( s, 
              p, 
              o
              ).hasNext();
        
    }

    //Gets all defined classes. 
    public StatementIterator getClasses()
    {

        return getStatements
            ( null,
              new URIImpl( RDF.TYPE ),
              new URIImpl( RDFS.CLASS )
              );
        
    }


    // Checks whether the supplied resource represents a class.
    public boolean isClass
        ( Resource resource
          )
    {
        
        return hasStatement
            ( resource, 
              new URIImpl( RDF.TYPE ), 
              new URIImpl( RDFS.CLASS )
              );
    
    }
    
    // Gets all defined properties. 
    public StatementIterator getProperties()
    {
        
        return getStatements
            ( null,
              new URIImpl( RDF.TYPE ),
              new URIImpl( RDF.PROPERTY )
              );
    
    }


    // Checks whether the supplied resource represents a property.
    public boolean isProperty
        ( Resource resource
          )
    {
        
        return hasStatement
            ( resource, 
              new URIImpl( RDF.TYPE ), 
              new URIImpl( RDF.PROPERTY )
              );
        
    }

    // Gets all subClassOf relations with a specific sub- and/or
    // superclass.
    public StatementIterator getSubClassOf
        ( Resource subClass,
          Resource superClass
          )
    {
        
        return getStatements
            ( subClass, 
              new URIImpl( RDFS.SUBCLASSOF ), 
              superClass
              );

    }

    // Checks whether one resource is a subclass of another.
    public boolean isSubClassOf
        ( Resource subClass,
          Resource superClass
          )
    {
    
        return hasStatement
            ( subClass, 
              new URIImpl( RDFS.SUBCLASSOF ), 
              superClass
              );

    }

    // Checks whether one resource is a direct subclass of another.
    public boolean isDirectSubClassOf
        ( Resource subClass,
          Resource superClass
          )
    {
        
        return getDirectSubClassOf
            ( subClass,
              superClass
              ).hasNext();
    
    }
    
    // Gets all subPropertyOf relations with a specific sub- and/or
    // superproperty.
    public StatementIterator getSubPropertyOf
        ( Resource subProperty,
          Resource superProperty
          )
    {
        
        return getStatements
            ( subProperty, 
              new URIImpl( RDFS.SUBPROPERTYOF ), 
              superProperty
              );

    }

    // Checks whether one resource is a subproperty of another.
    public boolean isSubPropertyOf
        ( Resource subProperty,
          Resource superProperty
          )
    {

        return hasStatement
            ( subProperty, 
              new URIImpl( RDFS.SUBPROPERTYOF ), 
              superProperty
              );

    }
    
    // Checks whether one resource is a direct subproperty of another.
    public boolean isDirectSubPropertyOf
        ( Resource subProperty,
          Resource superProperty
          )
    {

        return getDirectSubPropertyOf
            ( subProperty,
              superProperty
              ).hasNext();
        
    }
    
    // Gets all domain relations with a specific property and/or
    // domain class.
    public StatementIterator getDomain
        ( Resource prop,
          Resource domain
          )
    {
        
        return getStatements
            ( prop, 
              new URIImpl( RDFS.DOMAIN ), 
              domain
              );

    }


    // Gets all range relations with a specific property and/or range
    // class.
    public StatementIterator getRange
        ( Resource prop,
          Resource range
          )
    {
        
        return getStatements
            ( prop, 
              new URIImpl( RDFS.RANGE ), 
              range
              );

    }

    // Gets all type relations with a specific instance and/or class.
    public StatementIterator getType
        ( Resource anInstance,
          Resource aClass
          )
    {
        
        return getStatements
            ( anInstance, 
              new URIImpl( RDF.TYPE ), 
              aClass
              );

    }
    
    // Checks whether one resource is an instance of another. 
    public boolean isType
        ( Resource anInstance,
          Resource aClass
          )
    {

        return hasStatement
            ( anInstance, 
              new URIImpl( RDF.TYPE ), 
              aClass
              );

    }
    
    // Checks whether one resource is a direct instance of another.
    public boolean isDirectType
        ( Resource anInstance,
          Resource aClass
          )
    {
        
        return getDirectType
            ( anInstance,
              aClass
              ).hasNext();
        
    }

    /*
     * All the nasty methods.
     */
    
    public StatementIterator getDirectSubClassOf(Resource subClass, Resource superClass) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public StatementIterator getDirectSubPropertyOf(Resource subProperty, Resource superProperty) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public StatementIterator getDirectType(Resource anInstance, Resource aClass) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    public LiteralIterator getLiterals(String label, String language, URI datatype) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

//    private class GURI extends Object {};
//    private interface IGeneric {};
//    private Object getGraph() {return null;}
//    
//    // Gets all direct subClassOf relations with a specific sub-
//    // and/or superclass.
//    public StatementIterator getDirectSubClassOf
//        ( Resource subClass,
//          Resource superClass
//          )
//    {
//        
//        if ( subClass == null && superClass == null ) {
//            
//            return new EmptyStatementIterator();
//            
//        }
//        
//        GURI sco = ( GURI ) getGraph().lookup
//            ( new URIImpl( RDFS.SUBCLASSOF ) 
//              ); 
//        
//        if ( subClass != null && superClass != null ) {
//            
//            StatementIterator it = getDirectSub
//                ( superClass, 
//                  sco
//                  );
//            
//            while ( it.hasNext() ) {
//                
//                Statement stmt = it.next();
//                
//                if ( stmt.getSubject().equals( subClass ) ) {
//                    
//                    return new GStatementIterator
//                        ( Arrays.asList( new Statement[] { stmt } ).iterator()
//                          );
//                    
//                }
//                
//            }
//            
//            return new EmptyStatementIterator();
//            
//        } else if ( subClass != null ) {
//            
//            return getDirectSuper
//                ( subClass, 
//                  sco
//                  );
//            
//        } else if ( superClass != null ) {
//            
//            return getDirectSub
//                ( superClass, 
//                  sco
//                  );
//            
//        }
//        
//        return new EmptyStatementIterator();
//        
//    }
//    
//    /**
//     * explicit statements:
//     *   A sco B
//     *   B sco C
//     * implicit statements:
//     *   A sco A
//     *   B sco B
//     *   C sco C
//     *   A sco C
//     * query 1:
//     *   ?sub rdfs:subClassOf C
//     *   and ?sub ne C
//     * result set 1:
//     *   A sco C
//     *   B sco C
//     * query 2:
//     *   construct ?sub rdfs:subClassOf C
//     *   ?sub rdfs:subClassOf ?mid
//     *   ?mid rdfs:subClassOf C
//     *   and ?sub ne ?mid &&
//     *       ?mid ne C
//     * result set 2:
//     *   A sco C
//     *  
//     * @param superClass
//     * @return
//     */
//    private StatementIterator getDirectSub
//        ( Resource superResource, 
//          GURI predicate
//          )
//    {
//        
//        superResource = ( Resource ) getGraph().lookup( superResource );
//    
//        if( superResource == null ) {
//    
//            // If the URI is not already part of the graph, then the
//            // join MUST be empty.
//    
//            log.debug
//                ( "The specified class does not occur in this Graph."
//                  );
//    
//            return new EmptyStatementIterator();
//
//        }
//        
//        return getDirectSub
//            ( ( GURI ) superResource, 
//              predicate
//              );
//        
//    }
//    
//    /**
//     * Find all: 
//     *   X predicate Z 
//     * Where not:
//     *   X predicate Y
//     *   Y predicate Z
//     * 
//     * @param superGeneric
//     * @return
//     */
//    
//    private StatementIterator getDirectSub
//        ( GURI superURI, 
//          GURI predicate
//          )
//    {
//
//        IGeneric superGeneric = superURI.asGeneric();
//        
//        IGeneric graph = getGraph().asGeneric();
//
//        // Create a new generic object that will collect the query
//        // results in a link set.
//    
//        final IGeneric resultObject = graph.makeObject();
//    
//        final ILinkSet resultLS = resultObject.getLinkSet
//            ( "result-statements"
//              );
//        
//        final ILinkSet intermediateLS = resultObject.getLinkSet
//            ( "intermediate-statements"
//              );        
//       
//        ILinkSet predicateLS = predicate.asGeneric().getLinkSet
//            ( RDFPropertySchema.PREDICATE
//              );
//        
//        // first get all the non-reflexive statements into a link set
//        
//        Iterator it1 = predicateLS.getJoin().add
//            ( superGeneric.getLinkSet
//                  ( RDFPropertySchema.OBJECT
//                    )
//              ).getIntersection();
//        
//        while ( it1.hasNext() ) {
//            
//            IGeneric stmt = ( IGeneric ) it1.next();
//            
//            IGeneric subGeneric = stmt.getLink
//                ( RDFPropertySchema.SUBJECT
//                  );
//            
//            // no reflexive results please
//            if ( subGeneric.identity().equals( superGeneric.identity() ) ) {
//                
//                continue;
//                
//            }
//            
//            log.debug( "found a sub: " + 
//                       subGeneric.getString( RDFPropertySchema.URI ) );
//            
//            intermediateLS.add
//                ( stmt
//                  );
//            
//            resultLS.add
//                ( stmt
//                  );
//                
//        }
//        
//        // next find the indirect sub relationships and remove them from the
//        // results link set
//        
//        for ( Iterator it2 = intermediateLS.iterator(); it2.hasNext(); ) {
//            
//            IGeneric stmt = ( IGeneric ) it2.next();
//            
//            IGeneric subGeneric = stmt.getLink
//                ( RDFPropertySchema.SUBJECT
//                  );
//            
//            // for each sub, find the statements in which it participates
//            // as the object
//            
//            Iterator it3 = predicateLS.getJoin().add
//                ( subGeneric.getLinkSet
//                      ( RDFPropertySchema.OBJECT
//                        )
//                  ).getIntersection();
//            
//            while ( it3.hasNext() ) {
//                
//                IGeneric stmt2 = ( IGeneric ) it3.next();
//                
//                IGeneric s = stmt2.getLink
//                    ( RDFPropertySchema.SUBJECT
//                      );
//                
//                // we don't want reflexive statements here either
//                
//                if ( s.identity().equals( subGeneric.identity() ) ) {
//                    
//                    continue;
//                    
//                }
//                
//                log.debug( "found an indirect sub: " + 
//                           s.getString( RDFPropertySchema.URI ) );
//                
//                Iterator it4 = intermediateLS.getJoin().add
//                    ( s.getLinkSet
//                          ( RDFPropertySchema.SUBJECT
//                            )
//                      ).getIntersection();
//                
//                while ( it4.hasNext() ) {
//                
//                    IGeneric stmt3 = ( IGeneric ) it4.next();
//                    
//                    resultLS.remove
//                        ( stmt3
//                          );
//                    
//                }
//                
//            }
//            
//        }
//        
//        return new GStatementIterator
//            ( resultLS.iterator()
//              ) 
//        {
//            
//            public void close()
//            {
//                
//                log.debug( "removing temporary results object" );
//                
//                resultLS.clear();
//                
//                intermediateLS.clear();
//                
//                resultObject.remove();
//                
//            }
//            
//        };
//        
//    }
//    
//    private StatementIterator getDirectSuper
//        ( Resource subResource, 
//          GURI predicate
//          )
//    {
//        
//        subResource = ( URI ) getGraph().lookup( subResource );
//    
//        if( subResource == null ) {
//    
//            // If the URI is not already part of the graph, then the
//            // join MUST be empty.
//    
//            log.debug
//            ( "The specified class does not occur in this Graph."
//              );
//    
//            return new EmptyStatementIterator();
//
//        }
//        
//        return getDirectSuper
//            ( ( GURI ) subResource, 
//              predicate
//              );
//        
//    }
//    
//    /**
//     * Find all: 
//     *   X predicate Z 
//     * Where not:
//     *   X predicate Y
//     *   Y predicate Z
//     * 
//     * @param superClass
//     * @return
//     */
//    
//    private StatementIterator getDirectSuper
//        ( GURI subURI, 
//          GURI predicate
//          )
//    {
//
//        IGeneric subGeneric = subURI.asGeneric();
//        
//        IGeneric graph = getGraph().asGeneric();
//
//        // Create a new generic object that will collect the query
//        // results in a link set.
//    
//        final IGeneric resultObject = graph.makeObject();
//    
//        final ILinkSet resultLS = resultObject.getLinkSet
//            ( "result-SCO-statements"
//              );
//        
//        final ILinkSet intermediateLS = resultObject.getLinkSet
//            ( "intermediate-SCO-statements"
//              );        
//       
//        ILinkSet predicateLS = predicate.asGeneric().getLinkSet
//            ( RDFPropertySchema.PREDICATE
//              );
//        
//        // first get all the non-reflexive statements into a link set
//        
//        Iterator it1 = predicateLS.getJoin().add
//            ( subGeneric.getLinkSet
//                  ( RDFPropertySchema.SUBJECT
//                    )
//              ).getIntersection();
//        
//        while ( it1.hasNext() ) {
//            
//            IGeneric stmt = ( IGeneric ) it1.next();
//            
//            IGeneric superGeneric = stmt.getLink
//                ( RDFPropertySchema.OBJECT
//                  );
//            
//            // no reflexive results please
//            if ( superGeneric.identity().equals( subGeneric.identity() ) ) {
//                
//                continue;
//                
//            }
//            
//            log.debug( "found a super: " + 
//                       superGeneric.getString( RDFPropertySchema.URI ) );
//            
//            intermediateLS.add
//                ( stmt
//                  );
//            
//            resultLS.add
//                ( stmt
//                  );
//                
//        }
//        
//        // next find the indirect relationships and remove them from the
//        // results link set
//        
//        for ( Iterator it2 = intermediateLS.iterator(); it2.hasNext(); ) {
//            
//            IGeneric stmt = ( IGeneric ) it2.next();
//            
//            IGeneric superGeneric = stmt.getLink
//                ( RDFPropertySchema.OBJECT
//                  );
//            
//            // for each super, find the statements in which it participates
//            // as the subject
//            
//            Iterator it3 = predicateLS.getJoin().add
//                ( superGeneric.getLinkSet
//                      ( RDFPropertySchema.SUBJECT
//                        )
//                  ).getIntersection();
//            
//            while ( it3.hasNext() ) {
//                
//                IGeneric stmt2 = ( IGeneric ) it3.next();
//                
//                IGeneric o = stmt2.getLink
//                    ( RDFPropertySchema.OBJECT
//                      );
//                
//                // we don't want reflexive SCO statements here either
//                
//                if ( o.identity().equals( superGeneric.identity() ) ) {
//                    
//                    continue;
//                    
//                }
//                
//                log.debug( "found an indirect super: " + 
//                           o.getString( RDFPropertySchema.URI ) );
//                
//                // find the indirect super in the intermediate results
//                
//                Iterator it4 = intermediateLS.getJoin().add
//                    ( o.getLinkSet
//                          ( RDFPropertySchema.OBJECT
//                            )
//                      ).getIntersection();
//                
//                // and remove it from the final results
//                
//                while ( it4.hasNext() ) {
//                
//                    IGeneric stmt3 = ( IGeneric ) it4.next();
//                    
//                    resultLS.remove
//                        ( stmt3
//                          );
//                    
//                }
//                
//            }
//            
//        }
//        
//        return new GStatementIterator
//            ( resultLS.iterator()
//              ) 
//        {
//            
//            public void close()
//            {
//                
//                log.debug( "removing temporary results object" );
//                
//                resultLS.clear();
//                
//                intermediateLS.clear();
//                
//                resultObject.remove();
//                
//            }
//            
//        };
//        
//    }
//
//    // Gets all direct subPropertyOf relations with a specific sub-
//    // and/or superproperty.
//    public StatementIterator getDirectSubPropertyOf
//        ( Resource subProperty,
//          Resource superProperty
//          ) 
//    {
//        
//        if ( subProperty == null && superProperty == null ) {
//            
//            return new EmptyStatementIterator();
//            
//        }
//        
//        GURI spo = ( GURI ) getGraph().lookup
//            ( new URIImpl( RDFS.SUBPROPERTYOF ) 
//              );
//        
//        if ( subProperty != null && superProperty != null ) {
//            
//            StatementIterator it = getDirectSub
//                ( superProperty, 
//                  spo
//                  );
//            
//            while ( it.hasNext() ) {
//                
//                Statement stmt = it.next();
//                
//                if ( stmt.getSubject().equals( subProperty ) ) {
//                    
//                    return new GStatementIterator
//                        ( Arrays.asList( new Statement[] { stmt } ).iterator()
//                          );
//                    
//                }
//                
//            }
//            
//            return new EmptyStatementIterator();
//            
//        } else if ( subProperty != null ) {
//            
//            return getDirectSuper
//                ( subProperty, 
//                  spo
//                  );
//            
//        } else if ( superProperty != null ) {
//            
//            return getDirectSub
//                ( superProperty, 
//                  spo
//                  );
//            
//        }
//        
//        return new EmptyStatementIterator();
//        
//    }
//          
//    // Gets all direct type relations with a specific instance and/or
//    // class.
//    public StatementIterator getDirectType
//        ( Resource anInstance,
//          Resource aClass
//          ) 
//    {
//        
//        if ( anInstance == null && aClass == null ) {
//            
//            return new GStatementIterator();
//            
//        }
//        
//        if ( anInstance != null && aClass != null ) {
//            
//            StatementIterator it = getDirectTypes( anInstance );
//            
//            while ( it.hasNext() ) {
//                
//                Statement stmt = it.next();
//                
//                if ( stmt.getObject().equals( aClass ) ) {
//                    
//                    return new GStatementIterator( stmt );
//                    
//                }
//                
//            }
//            
//            return new GStatementIterator();
//            
//        } else if ( anInstance != null ) {
//            
//            return getDirectTypes( anInstance );
//            
//        } else if ( aClass != null ) {
//            
//            return getDirectInstances( aClass );
//            
//        }
//        
//        return new GStatementIterator();
//
//    }
//
//    /**
//     * find all A
//     * where anInstance rdf:type A
//     * and not (anInstance rdf:type ?x) (?x rdfs:subClassOf A)
//     * and ?x ne A
//     * 
//     * @param anInstance
//     * @return
//     */
//    private StatementIterator getDirectTypes
//        ( Resource anInstance
//          )
//    {
//        
//        System.out.println( "getting direct types" );
//        
//        Map stmts = new HashMap();
//        
//        StatementIterator it = getStatements
//            ( anInstance,
//              URIImpl.RDF_TYPE,
//              null
//              );
//        
//        while ( it.hasNext() ) {
//            
//            Statement stmt = it.next();
//            
//            URI type = (URI) stmt.getObject();
//            
//            System.out.println( "adding type: " + type );
//            
//            stmts.put( type, stmt );
//            
//        }
//        
//        it.close();
//        
//        Collection c = new LinkedList();
//        
//        c.addAll( stmts.keySet() );
//        
//        Iterator it2 = c.iterator();
//        
//        while ( it2.hasNext() ) {
//            
//            URI type = (URI) it2.next();
//            
//            StatementIterator it3 = getStatements
//                ( type,
//                  URIImpl.RDFS_SUBCLASSOF,
//                  null
//                  );
//            
//            while ( it3.hasNext() ) {
//                
//                Statement stmt = it3.next();
//                
//                URI superType = (URI) stmt.getObject();
//                
//                if ( !type.equals( superType ) ) { 
//                    
//                    System.out.println( "removing type: " + superType );
//                    
//                    stmts.remove( superType );
//                    
//                }
//                
//            }
//            
//        }
//        
//        return new GStatementIterator( stmts.values() );
//
//    }
//    
//    /**
//     * find all A 
//     * where A rdf:type aClass 
//     * and not (A rdf:type ?x) (?x rdfs:subClassOf aClass)
//     * and ?x ne aClass
//     * 
//     * @param aClass
//     * @return
//     */
//    
//    private StatementIterator getDirectInstances
//        ( Resource aClass
//          )
//    {
//        
//        Map stmts = new HashMap();
//        
//        StatementIterator it = getStatements
//            ( null,
//              new URIImpl( RDF.TYPE ),
//              aClass
//              );
//        
//        while ( it.hasNext() ) {
//            
//            GStatement stmt = ( GStatement ) it.next();
//            
//            Resource instance = stmt.getSubject();
//            
//            stmts.put
//                ( instance,
//                  stmt.asGeneric()
//                  );
//            
//        }
//        
//        Collection instances = new LinkedList();
//        
//        instances.addAll
//            ( stmts.keySet()
//              );
//        
//        Iterator it2 = instances.iterator();
//        
//        while ( it2.hasNext() ) {
//            
//            Resource instance = ( Resource ) it2.next();
//            
//            StatementIterator it3 = getStatements
//                ( instance,
//                  new URIImpl( RDF.TYPE ),
//                  null
//                  );
//            
//            while ( it3.hasNext() ) {
//                
//                Statement stmt = it3.next();
//                
//                URI type = ( URI ) stmt.getObject();
//                
//                if ( !type.equals( aClass ) ) {
//
//                    boolean isSubclass = hasStatement
//                        ( type,
//                          new URIImpl( RDFS.SUBCLASSOF ),
//                          aClass
//                          );
//                
//                    if ( isSubclass ) { 
//                    
//                        stmts.remove
//                            ( instance
//                              );
//                    
//                    }
//                    
//                }
//                
//            }
//            
//        }
//        
//        return new GStatementIterator
//            ( stmts.values().iterator()
//              );
//
//    }
//    
//    /**
//     * @todo Why is this not on the {@link RdfRepository} interface?
//     */
//    // Gets all literals with a specific label, language and/or
//    // datatype.
//    public LiteralIterator getLiterals
//        ( String label,
//          String language,
//          URI datatype
//          )
//    {
//        
//        Collection literals = new LinkedList();
//        
//        ILinkSetIndex ndx = getGraph().asGeneric().getLinkSet
//            ( RDFPropertySchema.LITERAL_LINKSET
//              ).getIndex( RDFPropertySchema.LITERAL
//                  );
//    
//        Iterator itr = ndx.getPoints
//            ( label
//              );
//    
//        while( itr.hasNext() ) {
//    
//            IGeneric gx = ( IGeneric ) itr.next();
//    
//            Literal lit = ( Literal ) gx.asClass
//                ( GLiteral.class
//                  );
//    
//            if ( language != null && datatype != null &&
//                 language.equals( lit.getLanguage() ) &&
//                 datatype.equals( lit.getDatatype() ) ) {
//                    
//                literals.add
//                    ( gx
//                      );
//                    
//            } else if ( language != null && 
//                        language.equals( lit.getLanguage() ) &&
//                        lit.getDatatype() == null ) {
//                
//                literals.add
//                    ( gx
//                      );
//                
//            } else if ( datatype != null && 
//                        datatype.equals( lit.getDatatype() ) &&
//                        lit.getLanguage() == null ) {
//                
//                literals.add
//                    ( gx
//                      );
//                
//            } else if ( lit.getDatatype() == null &&
//                        lit.getLanguage() == null ){
//                
//                literals.add
//                    ( gx
//                      );
//                
//            }
//    
//        }
//        
//        return new GLiteralIterator
//            ( literals.iterator()
//              );
//        
//    }

}
