package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedup.asks.ASKParallel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import se.liu.ida.hefquin.base.query.impl.TriplePatternImpl;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.federation.access.TriplePatternRequest;
import se.liu.ida.hefquin.engine.federation.access.impl.req.TriplePatternRequestImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlanUtils;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalOpRequest;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.LogicalPlanWithNullaryRootImpl;
import se.liu.ida.hefquin.engine.queryproc.QueryProcContext;
import se.liu.ida.hefquin.engine.queryproc.SourcePlanner;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * This implementation of {@link SourcePlanner} assigns every triple
 * pattern of the given query to every federation member known in the
 * federation catalog. In other words, this source planner creates what
 * we call an 'exhaustive source assignment' in the FedQPL paper (see
 * Definition 9 in Section 5.2 of the paper).
 *
 * This source planner assumes that the given queries do not contain
 * any SERVICE clauses.
 */
public class TriplePatternAwareSourcePlannerImpl extends ExhaustiveSourcePlannerImpl
{
	private ASKParallel askVisitor;
	private Map<ImmutablePair<String, Triple>, Boolean> askToResult;

	public TriplePatternAwareSourcePlannerImpl(QueryProcContext ctxt) {
		super(ctxt);


		this.askVisitor = new ASKParallel(
				ctxt.getFederationCatalog()
						.getAllFederationMembers()
						.stream()
						.map(fm -> ((SPARQLEndpoint) fm).getInterface().getURL())
						.collect(Collectors.toSet()),
				t -> true);



	}

	protected LogicalPlan createPlanForBGP( final OpBGP bgpOp ) {

		try {
			FileWriter myWriter = new FileWriter("/Users/boisteau-desdevises-e/Documents/fraw-xp/write_query.sparql");
			myWriter.write("No RSA available for Tripe Pattern Aware Source Selection");
			myWriter.close();
		} catch (IOException e) {
//                    System.out.println("An error occurred.");
//                    e.printStackTrace();
		}

		askVisitor.execute(bgpOp.getPattern().getList());

		askToResult = askVisitor.getAsks();

		final BasicPattern bgp = bgpOp.getPattern();
		assert ! bgp.isEmpty();

		if ( bgp.size() == 1 ) {
			return createSubPlanForTP( bgp.get(0) );
		}

		final List<LogicalPlan> subPlans = new ArrayList<>();

		for ( final Triple tp : bgp.getList() ) {
			LogicalPlan sp = createSubPlanForTP( tp );
			if (Objects.nonNull(sp)) subPlans.add( sp );
		}

		if ( subPlans.isEmpty() ) return null;

		return LogicalPlanUtils.createPlanWithMultiwayJoin(subPlans);
	}

	protected LogicalPlan createSubPlanForTP( final Triple tp ) {
		final Set<FederationMember> allFMs = ctxt.getFederationCatalog().getAllFederationMembers();
		assert ! allFMs.isEmpty();

		if ( allFMs.size() == 1 ) {
			return createRequestSubPlan( tp, allFMs.iterator().next() );
		}

		final List<LogicalPlan> reqSubPlans = new ArrayList<>();
		for ( final FederationMember fm : allFMs ) {

			Pair p = Pair.of(((SPARQLEndpoint) fm).getInterface().getURL(), tp);

			if(!askToResult.containsKey(p)) {
				throw new IllegalStateException("Triple-endpoint combination wasn't tested ");
			}

			if(askToResult.get(p)) {
				reqSubPlans.add( createRequestSubPlan(tp, fm) );
			}
		}

		if ( reqSubPlans.isEmpty() ) return null; // TODO : move this in createPlanWithMultiwayUnion method

		return LogicalPlanUtils.createPlanWithMultiwayUnion(reqSubPlans);

	}

	protected LogicalPlan createRequestSubPlan( final Triple tp, final FederationMember fm ) {
		final TriplePatternRequest req = new TriplePatternRequestImpl( new TriplePatternImpl(tp) );
		final LogicalOpRequest<?,?> reqOp = new LogicalOpRequest<>(fm, req);
		return new LogicalPlanWithNullaryRootImpl(reqOp);
	}
}
