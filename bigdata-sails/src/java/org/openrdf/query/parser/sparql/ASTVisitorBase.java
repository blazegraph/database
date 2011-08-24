/*
 * Copyright Aduna (http://www.aduna-software.com/) (c) 1997-2006.
 *
 * Licensed under the Aduna BSD-style license.
 */
package org.openrdf.query.parser.sparql;

import org.openrdf.query.parser.sparql.ast.ASTAbs;
import org.openrdf.query.parser.sparql.ast.ASTAdd;
import org.openrdf.query.parser.sparql.ast.ASTAnd;
import org.openrdf.query.parser.sparql.ast.ASTAskQuery;
import org.openrdf.query.parser.sparql.ast.ASTAvg;
import org.openrdf.query.parser.sparql.ast.ASTBNodeFunc;
import org.openrdf.query.parser.sparql.ast.ASTBaseDecl;
import org.openrdf.query.parser.sparql.ast.ASTBasicGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTBind;
import org.openrdf.query.parser.sparql.ast.ASTBlankNode;
import org.openrdf.query.parser.sparql.ast.ASTBlankNodePropertyList;
import org.openrdf.query.parser.sparql.ast.ASTBound;
import org.openrdf.query.parser.sparql.ast.ASTCeil;
import org.openrdf.query.parser.sparql.ast.ASTClear;
import org.openrdf.query.parser.sparql.ast.ASTCoalesce;
import org.openrdf.query.parser.sparql.ast.ASTCollection;
import org.openrdf.query.parser.sparql.ast.ASTCompare;
import org.openrdf.query.parser.sparql.ast.ASTConcat;
import org.openrdf.query.parser.sparql.ast.ASTConstraint;
import org.openrdf.query.parser.sparql.ast.ASTConstruct;
import org.openrdf.query.parser.sparql.ast.ASTConstructQuery;
import org.openrdf.query.parser.sparql.ast.ASTContains;
import org.openrdf.query.parser.sparql.ast.ASTCopy;
import org.openrdf.query.parser.sparql.ast.ASTCount;
import org.openrdf.query.parser.sparql.ast.ASTCreate;
import org.openrdf.query.parser.sparql.ast.ASTDatasetClause;
import org.openrdf.query.parser.sparql.ast.ASTDatatype;
import org.openrdf.query.parser.sparql.ast.ASTDay;
import org.openrdf.query.parser.sparql.ast.ASTDeleteClause;
import org.openrdf.query.parser.sparql.ast.ASTDeleteData;
import org.openrdf.query.parser.sparql.ast.ASTDeleteWhere;
import org.openrdf.query.parser.sparql.ast.ASTDescribe;
import org.openrdf.query.parser.sparql.ast.ASTDescribeQuery;
import org.openrdf.query.parser.sparql.ast.ASTDrop;
import org.openrdf.query.parser.sparql.ast.ASTEncodeForURI;
import org.openrdf.query.parser.sparql.ast.ASTExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTFalse;
import org.openrdf.query.parser.sparql.ast.ASTFloor;
import org.openrdf.query.parser.sparql.ast.ASTFunctionCall;
import org.openrdf.query.parser.sparql.ast.ASTGraphGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTGraphOrDefault;
import org.openrdf.query.parser.sparql.ast.ASTGraphPatternGroup;
import org.openrdf.query.parser.sparql.ast.ASTGraphRefAll;
import org.openrdf.query.parser.sparql.ast.ASTGroupClause;
import org.openrdf.query.parser.sparql.ast.ASTGroupConcat;
import org.openrdf.query.parser.sparql.ast.ASTGroupCondition;
import org.openrdf.query.parser.sparql.ast.ASTHavingClause;
import org.openrdf.query.parser.sparql.ast.ASTHours;
import org.openrdf.query.parser.sparql.ast.ASTIRI;
import org.openrdf.query.parser.sparql.ast.ASTIRIFunc;
import org.openrdf.query.parser.sparql.ast.ASTIf;
import org.openrdf.query.parser.sparql.ast.ASTIn;
import org.openrdf.query.parser.sparql.ast.ASTInsertClause;
import org.openrdf.query.parser.sparql.ast.ASTInsertData;
import org.openrdf.query.parser.sparql.ast.ASTIsBlank;
import org.openrdf.query.parser.sparql.ast.ASTIsIRI;
import org.openrdf.query.parser.sparql.ast.ASTIsLiteral;
import org.openrdf.query.parser.sparql.ast.ASTIsNumeric;
import org.openrdf.query.parser.sparql.ast.ASTLang;
import org.openrdf.query.parser.sparql.ast.ASTLangMatches;
import org.openrdf.query.parser.sparql.ast.ASTLimit;
import org.openrdf.query.parser.sparql.ast.ASTLoad;
import org.openrdf.query.parser.sparql.ast.ASTLowerCase;
import org.openrdf.query.parser.sparql.ast.ASTMD5;
import org.openrdf.query.parser.sparql.ast.ASTMath;
import org.openrdf.query.parser.sparql.ast.ASTMax;
import org.openrdf.query.parser.sparql.ast.ASTMin;
import org.openrdf.query.parser.sparql.ast.ASTMinusGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTMinutes;
import org.openrdf.query.parser.sparql.ast.ASTModify;
import org.openrdf.query.parser.sparql.ast.ASTMonth;
import org.openrdf.query.parser.sparql.ast.ASTMove;
import org.openrdf.query.parser.sparql.ast.ASTNot;
import org.openrdf.query.parser.sparql.ast.ASTNotExistsFunc;
import org.openrdf.query.parser.sparql.ast.ASTNotIn;
import org.openrdf.query.parser.sparql.ast.ASTNow;
import org.openrdf.query.parser.sparql.ast.ASTNumericLiteral;
import org.openrdf.query.parser.sparql.ast.ASTObjectList;
import org.openrdf.query.parser.sparql.ast.ASTOffset;
import org.openrdf.query.parser.sparql.ast.ASTOptionalGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTOr;
import org.openrdf.query.parser.sparql.ast.ASTOrderClause;
import org.openrdf.query.parser.sparql.ast.ASTOrderCondition;
import org.openrdf.query.parser.sparql.ast.ASTPathAlternative;
import org.openrdf.query.parser.sparql.ast.ASTPathElt;
import org.openrdf.query.parser.sparql.ast.ASTPathMod;
import org.openrdf.query.parser.sparql.ast.ASTPathOneInPropertySet;
import org.openrdf.query.parser.sparql.ast.ASTPathSequence;
import org.openrdf.query.parser.sparql.ast.ASTPrefixDecl;
import org.openrdf.query.parser.sparql.ast.ASTProjectionElem;
import org.openrdf.query.parser.sparql.ast.ASTPropertyList;
import org.openrdf.query.parser.sparql.ast.ASTPropertyListPath;
import org.openrdf.query.parser.sparql.ast.ASTQName;
import org.openrdf.query.parser.sparql.ast.ASTQuadsNotTriples;
import org.openrdf.query.parser.sparql.ast.ASTQueryContainer;
import org.openrdf.query.parser.sparql.ast.ASTRDFLiteral;
import org.openrdf.query.parser.sparql.ast.ASTRand;
import org.openrdf.query.parser.sparql.ast.ASTRegexExpression;
import org.openrdf.query.parser.sparql.ast.ASTRound;
import org.openrdf.query.parser.sparql.ast.ASTSHA1;
import org.openrdf.query.parser.sparql.ast.ASTSHA224;
import org.openrdf.query.parser.sparql.ast.ASTSHA256;
import org.openrdf.query.parser.sparql.ast.ASTSHA384;
import org.openrdf.query.parser.sparql.ast.ASTSHA512;
import org.openrdf.query.parser.sparql.ast.ASTSameTerm;
import org.openrdf.query.parser.sparql.ast.ASTSample;
import org.openrdf.query.parser.sparql.ast.ASTSeconds;
import org.openrdf.query.parser.sparql.ast.ASTSelect;
import org.openrdf.query.parser.sparql.ast.ASTSelectQuery;
import org.openrdf.query.parser.sparql.ast.ASTStr;
import org.openrdf.query.parser.sparql.ast.ASTStrDt;
import org.openrdf.query.parser.sparql.ast.ASTStrEnds;
import org.openrdf.query.parser.sparql.ast.ASTStrLang;
import org.openrdf.query.parser.sparql.ast.ASTStrLen;
import org.openrdf.query.parser.sparql.ast.ASTStrStarts;
import org.openrdf.query.parser.sparql.ast.ASTString;
import org.openrdf.query.parser.sparql.ast.ASTSubstr;
import org.openrdf.query.parser.sparql.ast.ASTSum;
import org.openrdf.query.parser.sparql.ast.ASTTimezone;
import org.openrdf.query.parser.sparql.ast.ASTTriplesSameSubject;
import org.openrdf.query.parser.sparql.ast.ASTTriplesSameSubjectPath;
import org.openrdf.query.parser.sparql.ast.ASTTrue;
import org.openrdf.query.parser.sparql.ast.ASTTz;
import org.openrdf.query.parser.sparql.ast.ASTUnionGraphPattern;
import org.openrdf.query.parser.sparql.ast.ASTUpdate;
import org.openrdf.query.parser.sparql.ast.ASTUpdateContainer;
import org.openrdf.query.parser.sparql.ast.ASTUpdateSequence;
import org.openrdf.query.parser.sparql.ast.ASTUpperCase;
import org.openrdf.query.parser.sparql.ast.ASTVar;
import org.openrdf.query.parser.sparql.ast.ASTWhereClause;
import org.openrdf.query.parser.sparql.ast.ASTYear;
import org.openrdf.query.parser.sparql.ast.SimpleNode;
import org.openrdf.query.parser.sparql.ast.SyntaxTreeBuilderVisitor;
import org.openrdf.query.parser.sparql.ast.VisitorException;

/**
 * Base class for visitors of the SPARQL AST.
 * 
 * @author arjohn
 */
public abstract class ASTVisitorBase implements SyntaxTreeBuilderVisitor {

	public Object visit(ASTAbs node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTUpdateSequence node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTUpdateContainer node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTAdd node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTClear node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTCopy node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTCreate node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDeleteClause node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDeleteData node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDeleteWhere node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDrop node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTGraphOrDefault node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTGraphRefAll node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTInsertClause node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTInsertData node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTLoad node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTModify node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMove node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTNow node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTYear node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMonth node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDay node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTHours node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTTz node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMinutes node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSeconds node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTTimezone node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTAnd node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTAskQuery node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTAvg node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMD5 node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSHA1 node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSHA224 node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSHA256 node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSHA384 node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSHA512 node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTBaseDecl node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTBasicGraphPattern node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTBind node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTBlankNode node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTBlankNodePropertyList node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTBNodeFunc node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTBound node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTCeil node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTCoalesce node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTConcat node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTContains node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTCollection node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTCompare node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTConstraint node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTConstruct node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTConstructQuery node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTCount node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDatasetClause node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDatatype node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDescribe node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTDescribeQuery node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTExistsFunc node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTEncodeForURI node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTFalse node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTFloor node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTFunctionCall node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTGraphGraphPattern node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTGraphPatternGroup node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTGroupClause node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTGroupConcat node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTGroupCondition node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTHavingClause node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIf node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIn node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIRI node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIRIFunc node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIsBlank node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIsIRI node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIsLiteral node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTIsNumeric node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTLang node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTLangMatches node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTLimit node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTLowerCase node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMath node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMax node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMin node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTMinusGraphPattern node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTNot node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTNotExistsFunc node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTNotIn node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTNumericLiteral node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTObjectList node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTOffset node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTOptionalGraphPattern node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTOr node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTOrderClause node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTOrderCondition node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPathAlternative node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPathElt node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPathMod node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPathOneInPropertySet node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPathSequence node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPrefixDecl node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTProjectionElem node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPropertyList node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTPropertyListPath node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTQName node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTQueryContainer node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTRand node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTRDFLiteral node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTRegexExpression node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTRound node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSameTerm node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSample node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSelect node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSelectQuery node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTStr node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTStrDt node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTStrEnds node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTString node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTStrLang node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTStrLen node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTStrStarts node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSubstr node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTSum node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTTriplesSameSubject node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTTriplesSameSubjectPath node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTQuadsNotTriples node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTTrue node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTUnionGraphPattern node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTUpdate node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTUpperCase node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTVar node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(ASTWhereClause node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

	public Object visit(SimpleNode node, Object data)
		throws VisitorException
	{
		return node.childrenAccept(this, data);
	}

}
