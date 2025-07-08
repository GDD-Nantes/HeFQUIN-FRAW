package se.liu.ida.hefquin.base.query.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import java.util.Iterator;
import java.util.Set;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.Vars;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprTransformSubstitute;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.graph.NodeTransformLib;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementGroup;
import org.apache.jena.sparql.syntax.ElementTriplesBlock;
import org.apache.jena.sparql.syntax.ElementUnion;
import org.apache.jena.sparql.syntax.syntaxtransform.ElementTransform;
import org.apache.jena.sparql.syntax.syntaxtransform.ElementTransformSubst;
import org.apache.jena.sparql.syntax.syntaxtransform.ElementTransformer;
import org.apache.jena.sparql.syntax.syntaxtransform.NodeTransformSubst;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.query.*;
import se.liu.ida.hefquin.base.query.impl.*;

import java.util.*;
import se.liu.ida.hefquin.base.query.impl.TriplePatternImpl;

public class QueryPatternUtils
{
	/**
	 * Returns a representation of the given graph pattern as
	 * an object of the {@link Op} interface of the Jena API.
	 */
	public static Op convertToJenaOp( final SPARQLGraphPattern pattern ) {
		if ( pattern instanceof TriplePattern tp) {
			return new OpTriple( tp.asJenaTriple() );
		}
		else if ( pattern instanceof BGP bgp ) {
			final Set<TriplePattern> tps = bgp.getTriplePatterns();
			final BasicPattern bp = new BasicPattern();
			for ( final TriplePattern tp : tps ) {
				bp.add( tp.asJenaTriple() );
			}
			return new OpBGP(bp);
		}
		else if ( pattern instanceof SPARQLUnionPattern up ) {
			final Iterator<SPARQLGraphPattern> it = up.getSubPatterns().iterator();
			Op unionOp = convertToJenaOp( it.next() );
			while ( it.hasNext() ) {
				final Op nextOp = convertToJenaOp( it.next() );
				unionOp = OpUnion.create( unionOp, nextOp );
			}

			return unionOp;
		}
		else if ( pattern instanceof SPARQLGroupPattern gp ) {
			final Iterator<SPARQLGraphPattern> it = gp.getSubPatterns().iterator();
			Op joinOp = convertToJenaOp( it.next() );
			while ( it.hasNext() ) {
				final Op nextOp = convertToJenaOp( it.next() );
				joinOp = OpJoin.create( joinOp, nextOp );
			}

			return joinOp;
		}
		else if ( pattern instanceof GenericSPARQLGraphPatternImpl1 gp1 ) {
			@SuppressWarnings("deprecation")
			final Op jenaOp = gp1.asJenaOp();
			return jenaOp;
		}
		else if ( pattern instanceof GenericSPARQLGraphPatternImpl2 gp2 ) {
			return gp2.asJenaOp();
		}

		throw new IllegalArgumentException( "Unsupported type of graph pattern: " + pattern.getClass().getName() );
	}

	public static Element convertToJenaElement( final SPARQLGraphPattern p ) {
		if ( p instanceof TriplePattern tp ) {
			final ElementTriplesBlock e = new ElementTriplesBlock();
			e.addTriple( tp.asJenaTriple() );
			return e;
		}
		else if ( p instanceof BGP bgp ) {
			final ElementTriplesBlock e = new ElementTriplesBlock();
			for ( final TriplePattern tp : bgp.getTriplePatterns() ) {
				e.addTriple( tp.asJenaTriple() );
			}
			return e;
		}
		else if (p instanceof SPARQLUnionPattern up ) {
			final ElementUnion e = new ElementUnion();
			for ( final SPARQLGraphPattern gp : up.getSubPatterns() ) {
				e.addElement(convertToJenaElement(gp));
			}
			return e;
		}
		else if (p instanceof SPARQLGroupPattern gp ) {
			final ElementGroup e = new ElementGroup();
			for ( final SPARQLGraphPattern g : gp.getSubPatterns() ) {
				e.addElement(convertToJenaElement(g));
			}
			return e;
		}
		else if ( p instanceof GenericSPARQLGraphPatternImpl1 gp1 ) {
			return gp1.asJenaElement();
		}
		else if ( p instanceof GenericSPARQLGraphPatternImpl2 gp2 ) {
			@SuppressWarnings("deprecation")
			final Element jenaElement = gp2.asJenaElement();
			return jenaElement;
		}
		else {
			throw new IllegalArgumentException( "unexpected type of graph pattern: " + p.getClass().getName() );
		}
	}

	/**
	 * Returns the set of all variables that occur in the given graph pattern,
	 * but ignoring variables in FILTER expressions.
	 *
	 * If the given pattern is a {@link TriplePattern}, this function returns
	 * the result of {@link #getVariablesInPattern(TriplePattern)}. Similarly,
	 * if the given pattern is a {@link BGP}, this function returns the result
	 * of {@link #getVariablesInPattern(BGP)}.
	 */
	public static Set<Var> getVariablesInPattern( final SPARQLGraphPattern queryPattern ) {
		if ( queryPattern instanceof TriplePattern ) {
			return getVariablesInPattern( (TriplePattern) queryPattern );
		}
		else if ( queryPattern instanceof BGP ) {
			return getVariablesInPattern( (BGP) queryPattern );
		}
		else if ( queryPattern instanceof SPARQLGroupPattern ) {
			final SPARQLGroupPattern up = (SPARQLGroupPattern) queryPattern;
			Set<Var> vars = new HashSet<>();
			for ( int i = 0; i < up.getNumberOfSubPatterns(); i++ ) {
				vars.addAll( getVariablesInPattern( up.getSubPatterns(i) ) );
			}

			return vars;
		}
		else if ( queryPattern instanceof SPARQLUnionPattern ) {
			final SPARQLUnionPattern up = (SPARQLUnionPattern) queryPattern;
			Set<Var> vars = new HashSet<>();
			for ( int i = 0; i < up.getNumberOfSubPatterns(); i++ ) {
				vars.addAll( getVariablesInPattern( up.getSubPatterns(i) ) );
			}

			return vars;
		}
		else if ( queryPattern instanceof GenericSPARQLGraphPatternImpl1 ) {
			@SuppressWarnings("deprecation")
			final Op jenaOp = ( (GenericSPARQLGraphPatternImpl1) queryPattern ).asJenaOp();
			return getVariablesInPattern(jenaOp);
		}
		else if ( queryPattern instanceof GenericSPARQLGraphPatternImpl2 ) {
			final Op jenaOp = ( (GenericSPARQLGraphPatternImpl2) queryPattern ).asJenaOp();
			return getVariablesInPattern(jenaOp);
		}
		else {
			throw new UnsupportedOperationException( queryPattern.getClass().getName() );
		}
	}

	public static Set<Var> getVariablesInPattern( final TriplePattern tp ) {
		return getVariablesInPattern( tp.asJenaTriple() );
	}

	public static Set<Var> getVariablesInPattern( final Triple tp ) {
		final Set<Var> result = new HashSet<>();
		Vars.addVarsFromTriple( result, tp );
		return result;
	}

	public static Set<Var> getVariablesInPattern( final BGP bgp ) {
		final Set<Var> result = new HashSet<>();
		for ( final TriplePattern tp : bgp.getTriplePatterns() ) {
			result.addAll( getVariablesInPattern(tp) );
		}
		return result;
	}

	public static Set<Var> getVariablesInPattern( final OpBGP bgp ) {
		final Set<Var> result = new HashSet<>();
		for ( final Triple tp : bgp.getPattern().getList() ) {
			result.addAll( getVariablesInPattern(tp) );
		}
		return result;
	}

	public static Set<Var> getVariablesInPattern( final Op2 op ) {
		final Set<Var> varLeft = getVariablesInPattern( op.getLeft() );
		final Set<Var> varRight = getVariablesInPattern( op.getRight() );
		varLeft.addAll(varRight);
		return varLeft;
	}

	public static Set<Var> getVariablesInPattern( final OpSequence op) {
		final Set<Var> result = new HashSet<>();
		op.getElements().stream().forEach(e -> result.addAll(getVariablesInPattern(e)));
		return result;
	}

	/**
	 * Ignores variables in FILTER expressions.
	 */
	public static Set<Var> getVariablesInPattern( final Op op ) {
		if ( op instanceof OpBGP ) {
			return getVariablesInPattern( (OpBGP) op);
		}
		else if ( op instanceof OpJoin || op instanceof OpLeftJoin || op instanceof OpUnion ) {
			return getVariablesInPattern( (Op2) op );
		}
		else if ( op instanceof OpService ){
			return getVariablesInPattern( ((OpService) op).getSubOp());
		}
		else if ( op instanceof OpFilter ){
			return getVariablesInPattern( ((OpFilter) op).getSubOp());
		}
		else if ( op instanceof OpSequence ){
			return getVariablesInPattern( (OpSequence) op );
		}
		else if (op instanceof OpTriple) {
			return getVariablesInPattern( ((OpTriple) op).getTriple() );
		} else {
			throw new UnsupportedOperationException("Getting the variables from arbitrary SPARQL patterns is an open TODO (type of Jena Op in the current case: " + op.getClass().getName() + ").");
		}
	}


	public static SPARQLGraphPattern applySolMapToGraphPattern(
			final SolutionMapping sm,
			final SPARQLGraphPattern pattern )
			throws VariableByBlankNodeSubstitutionException
	{
		if ( pattern instanceof TriplePattern )
		{
			return applySolMapToTriplePattern( sm, (TriplePattern) pattern );
		}
		else if ( pattern instanceof BGP )
		{
			return applySolMapToBGP( sm, (BGP) pattern );
		}
		else if ( pattern instanceof SPARQLUnionPattern )
		{
			final SPARQLUnionPattern up = (SPARQLUnionPattern) pattern;
			final SPARQLUnionPatternImpl upNew = new SPARQLUnionPatternImpl();
			boolean unchanged = true;
			for ( final SPARQLGraphPattern p : up.getSubPatterns() ) {
				final SPARQLGraphPattern pNew = applySolMapToGraphPattern(sm, p);
				upNew.addSubPattern(pNew);
				if ( ! pNew.equals(p) ) {
					unchanged = false;
				}
			}

			return ( unchanged ) ? pattern : upNew;
		}
		else if ( pattern instanceof SPARQLGroupPattern )
		{
			final SPARQLGroupPattern gp = (SPARQLGroupPattern) pattern;
			final SPARQLGroupPatternImpl gpNew = new SPARQLGroupPatternImpl();
			boolean unchanged = true;
			for ( final SPARQLGraphPattern p : gp.getSubPatterns() ) {
				final SPARQLGraphPattern pNew = applySolMapToGraphPattern(sm, p);
				gpNew.addSubPattern(pNew);
				if ( ! pNew.equals(p) ) {
					unchanged = false;
				}
			}

			return ( unchanged ) ? pattern : gpNew;
		}
		else if ( pattern instanceof GenericSPARQLGraphPatternImpl1 )
		{
			final Map<Var, Node> map1 = new HashMap<>();
			final Map<String, Expr> map2 = new HashMap<>();
			sm.asJenaBinding().forEach( (v,n) -> { map1.put(v,n); map2.put(v.getVarName(), NodeValue.makeNode(n)); } );
			final ElementTransform t1 = new ElementTransformSubst(map1);
			final ExprTransformSubstitute t2 = new ExprTransformSubstitute(map2);

			final Element e = ( (GenericSPARQLGraphPatternImpl1) pattern ).asJenaElement();
			final Element eNew = ElementTransformer.transform(e, t1, t2);
			return ( e == eNew ) ? pattern : new GenericSPARQLGraphPatternImpl1(eNew);
		}
		else if ( pattern instanceof GenericSPARQLGraphPatternImpl2 )
		{
			final Map<Var, Node> map = new HashMap<>();
			sm.asJenaBinding().forEach( (v,n) -> map.put(v,n) );
			final NodeTransform t = new NodeTransformSubst(map);

			final Op op = ( (GenericSPARQLGraphPatternImpl2) pattern ).asJenaOp();
			final Op opNew = NodeTransformLib.transform(t, op);
			return ( op == opNew ) ? pattern : new GenericSPARQLGraphPatternImpl2(opNew);
		}
		else
			throw new UnsupportedOperationException("TODO");
	}

	/**
	 * Attention, this function throws an exception in all cases in which
	 * one of the variables of the BGP would be replaced by a blank node.
	 */
	public static BGP applySolMapToBGP( final SolutionMapping sm, final BGP bgp )
			throws VariableByBlankNodeSubstitutionException
	{
		final Set<TriplePattern> tps = new HashSet<>();
		boolean unchanged = true;
		for ( final TriplePattern tp : ((BGPImpl)bgp).getTriplePatterns() ) {
			final TriplePattern tp2 = applySolMapToTriplePattern(sm, tp);
			tps.add(tp2);
			if ( tp2 != tp ) {
				unchanged = false;
			}
		}

		if ( unchanged ) {
			return bgp;
		} else {
			return new BGPImpl(tps);
		}
	}

	/**
	 * Attention, this function throws an exception in all cases in which one
	 * of the variables of the triple pattern would be replaced by a blank node.
	 */
	public static TriplePattern applySolMapToTriplePattern( final SolutionMapping sm,
															final TriplePattern tp )
			throws VariableByBlankNodeSubstitutionException
	{
		final Binding b = sm.asJenaBinding();
		boolean unchanged = true;

		Node s = tp.asJenaTriple().getSubject();
		if ( Var.isVar(s) ) {
			final Var var = Var.alloc(s);
			if ( b.contains(var) ) {
				s = b.get(var);
				unchanged = false;
				if ( s.isBlank() ) {
					throw new VariableByBlankNodeSubstitutionException();
				}
			}
		}

		Node p = tp.asJenaTriple().getPredicate();
		if ( Var.isVar(p) ) {
			final Var var = Var.alloc(p);
			if ( b.contains(var) ) {
				p = b.get(var);
				unchanged = false;
				if ( p.isBlank() ) {
					throw new VariableByBlankNodeSubstitutionException();
				}
			}
		}

		Node o = tp.asJenaTriple().getObject();
		if ( Var.isVar(o) ) {
			final Var var = Var.alloc(o);
			if ( b.contains(var) ) {
				o = b.get(var);
				unchanged = false;
				if ( o.isBlank() ) {
					throw new VariableByBlankNodeSubstitutionException();
				}
			}
		}

		if ( unchanged ) {
			return tp;
		} else {
			return new TriplePatternImpl(s,p,o);
		}
	}
}
