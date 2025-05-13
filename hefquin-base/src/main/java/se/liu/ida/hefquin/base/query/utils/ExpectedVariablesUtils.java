package se.liu.ida.hefquin.base.query.utils;

import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVars;
import org.apache.jena.sparql.core.Var;
import se.liu.ida.hefquin.base.query.*;
import se.liu.ida.hefquin.base.query.impl.GenericSPARQLGraphPatternImpl1;
import se.liu.ida.hefquin.base.query.impl.GenericSPARQLGraphPatternImpl2;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static se.liu.ida.hefquin.base.query.utils.QueryPatternUtils.getVariablesInPattern;

public class ExpectedVariablesUtils
{
	/**
	 * Returns a set of all the certain variables in all the given
	 * {@link ExpectedVariables} objects. Returns null if no such
	 * object is given.
	 */
	public static Set<Var> unionOfCertainVariables( final ExpectedVariables ... e ) {
		if ( e.length == 0 ) {
			return null;
		}

		final Set<Var> result = new HashSet<>( e[0].getCertainVariables() );

		for ( int i = 1; i < e.length; ++i ) {
			result.addAll( e[i].getCertainVariables() );
		}

		return result;
	}

	/**
	 * Returns a set of all the possible variables in all the given
	 * {@link ExpectedVariables} objects. Returns null if no such
	 * object is given.
	 */
	public static Set<Var> unionOfPossibleVariables( final ExpectedVariables ... e ) {
		if ( e.length == 0 ) {
			return null;
		}

		final Set<Var> result = new HashSet<>( e[0].getPossibleVariables() );

		for ( int i = 1; i < e.length; ++i ) {
			result.addAll( e[i].getPossibleVariables() );
		}

		return result;
	}

	/**
	 * Returns a set of all the variables (certain and possible) in all
	 * the given {@link ExpectedVariables} objects. Returns null if no
	 * such object is given.
	 */
	public static Set<Var> unionOfAllVariables( final ExpectedVariables ... e ) {
		if ( e.length == 0 ) {
			return null;
		}

		final Set<Var> result = new HashSet<>( e[0].getCertainVariables() );
		result.addAll( e[0].getPossibleVariables() );

		for ( int i = 1; i < e.length; ++i ) {
			result.addAll( e[i].getCertainVariables() );
			result.addAll( e[i].getPossibleVariables() );
		}

		return result;
	}

	/**
	 * Returns an intersection of the sets of certain variables in all
	 * the given {@link ExpectedVariables} objects. Returns null if no
	 * such object is given.
	 */
	public static Set<Var> intersectionOfCertainVariables( final ExpectedVariables ... e ) {
		if ( e.length == 0 ) {
			return null;
		}

		final Set<Var> result = new HashSet<>( e[0].getCertainVariables() );

		for ( int i = 1; i < e.length; ++i ) {
			result.retainAll( e[i].getCertainVariables() );
		}

		return result;
	}

	/**
	 * Returns an intersection of the sets of possible variables in all
	 * the given {@link ExpectedVariables} objects. Returns null if no
	 * such object is given.
	 */
	public static Set<Var> intersectionOfPossibleVariables( final ExpectedVariables ... e ) {
		if ( e.length == 0 ) {
			return null;
		}

		final Set<Var> result = new HashSet<>( e[0].getPossibleVariables() );

		for ( int i = 1; i < e.length; ++i ) {
			result.retainAll( e[i].getPossibleVariables() );
		}

		return result;
	}

	/**
	 * Returns an intersection of the sets of all variables (certain and
	 * possible) in all the given {@link ExpectedVariables} objects.
	 * Returns null if no such object is given.
	 */
	public static Set<Var> intersectionOfAllVariables( final ExpectedVariables ... e ) {
		if ( e.length == 0 ) {
			return null;
		}

		final Set<Var> result = new HashSet<>( e[0].getCertainVariables() );
		result.addAll( e[0].getPossibleVariables() );

		for ( int i = 1; i < e.length; ++i ) {
			final Set<Var> allVarsInCurrentObject = unionOfAllVariables( e[i] );
			result.retainAll( allVarsInCurrentObject );
		}

		return result;
	}

	/**
	 * Returns an array of the {@link ExpectedVariables} objects of
	 * all graph patterns in the given list, in the order in which
	 * the patterns are listed.
	 */
	public static ExpectedVariables[] getExpectedVariables( final List<SPARQLGraphPattern> patterns ) {
		final ExpectedVariables[] e = new ExpectedVariables[patterns.size()];
		for ( int i = 0; i < patterns.size(); ++i ) {
			e[i] = patterns.get(i).getExpectedVariables();
		}
		return e;
	}

	public static ExpectedVariables getExpectedVariablesInPattern( final SPARQLGraphPattern pattern ) {
		if ( pattern instanceof TriplePattern) {
			return new ExpectedVariables() {
				@Override public Set<Var> getPossibleVariables() {
					return Collections.emptySet();
				}

				@Override public Set<Var> getCertainVariables() {
					return getVariablesInPattern( (TriplePattern) pattern );
				}
			};
		}
		else if ( pattern instanceof BGP) {
			return new ExpectedVariables() {
				@Override public Set<Var> getPossibleVariables() {
					return Collections.emptySet();
				}

				@Override public Set<Var> getCertainVariables() {
					return getVariablesInPattern( (BGP) pattern );
				}
			};
		}
		else if ( pattern instanceof SPARQLGroupPattern) {
			final SPARQLGroupPattern gp = (SPARQLGroupPattern) pattern;
			final ExpectedVariables[] evs = new ExpectedVariables[gp.getNumberOfSubPatterns()];
			for ( int i = 0; i < gp.getNumberOfSubPatterns(); i++ ) {
				evs[i] = getExpectedVariablesInPattern( gp.getSubPatterns(i) );
			}

			final Set<Var> certainVars = ExpectedVariablesUtils.unionOfCertainVariables(evs);
			final Set<Var> possibleVars = ExpectedVariablesUtils.unionOfPossibleVariables(evs);
			possibleVars.removeAll(certainVars);

			return new ExpectedVariables() {
				@Override public Set<Var> getPossibleVariables() { return possibleVars; }
				@Override public Set<Var> getCertainVariables() { return certainVars; }
			};
		}
		else if ( pattern instanceof SPARQLUnionPattern ) {
			final SPARQLUnionPattern up = (SPARQLUnionPattern) pattern;
			final ExpectedVariables[] evs = new ExpectedVariables[up.getNumberOfSubPatterns()];
			for ( int i = 0; i < up.getNumberOfSubPatterns(); i++ ) {
				evs[i] = getExpectedVariablesInPattern( up.getSubPatterns(i) );
			}

			final Set<Var> certainVars = ExpectedVariablesUtils.intersectionOfCertainVariables(evs);
			final Set<Var> possibleVars = ExpectedVariablesUtils.unionOfAllVariables(evs);
			possibleVars.removeAll(certainVars);

			return new ExpectedVariables() {
				@Override public Set<Var> getPossibleVariables() { return possibleVars; }
				@Override public Set<Var> getCertainVariables() { return certainVars; }
			};
		}
		else {
			final Op jenaOp;
			if ( pattern instanceof GenericSPARQLGraphPatternImpl1) {
				@SuppressWarnings("deprecation")
				final Op o = ( (GenericSPARQLGraphPatternImpl1) pattern ).asJenaOp();
				jenaOp = o;
			}
			else if ( pattern instanceof GenericSPARQLGraphPatternImpl2) {
				jenaOp = ( (GenericSPARQLGraphPatternImpl2) pattern ).asJenaOp();
			}
			else {
				throw new UnsupportedOperationException( pattern.getClass().getName() );
			}

			final Set<Var> certainVars = OpVars.fixedVars(jenaOp);
			Set<Var> possibleVars = OpVars.visibleVars(jenaOp);
			possibleVars.removeAll(certainVars);

			return new ExpectedVariables() {
				@Override public Set<Var> getPossibleVariables() { return possibleVars; }
				@Override public Set<Var> getCertainVariables() { return certainVars; }
			};
		}
	}

	public static ExpectedVariables getExpectedVariablesInQuery( final SPARQLQuery query ) {
		final Set<Var> vars = new HashSet<>( query.asJenaQuery().getProjectVars() );

		return new ExpectedVariables() {
			@Override public Set<Var> getPossibleVariables() { return vars; }
			@Override public Set<Var> getCertainVariables() { return Collections.emptySet(); }
		};
	}
}
