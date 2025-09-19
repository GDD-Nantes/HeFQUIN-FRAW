package se.liu.ida.hefquin.engine.queryplan.utils;

import se.liu.ida.hefquin.base.data.VocabularyMapping;
import se.liu.ida.hefquin.engine.queryplan.logical.BinaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.NaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.NullaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.UnaryLogicalOp;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryplan.physical.*;
import se.liu.ida.hefquin.engine.queryplan.physical.impl.*;
import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.FederationMemberAgglomeration;
import se.liu.ida.hefquin.federation.access.DataRetrievalRequest;

import java.util.List;

public class SamplingPhysicalPlanFactory extends PhysicalPlanFactory
{
	// --------- plans with nullary root operators -----------

	/**
	 * Creates a plan with a request operator as root operator.
	 */
	public static <R extends DataRetrievalRequest, M extends FederationMember>
	PhysicalPlan createPlanWithRequest( final LogicalOpRequest<R,M> lop ) {
		final NullaryPhysicalOp pop;

		if(lop.getFederationMember() instanceof FederationMemberAgglomeration){
			pop = new PhysicalOpFrawRequest((LogicalOpRequest<DataRetrievalRequest, FederationMemberAgglomeration>) lop);
		} else {
			pop = new PhysicalOpRequest<>(lop);
		}
		return createPlan(pop);
	}

	/**
	 * Creates a physical plan in which the root operator is the
	 * default physical operator for the given logical operator,
	 * as per {@link LogicalToPhysicalOpConverter}.
	 */
	public static PhysicalPlan createPlan( final NullaryLogicalOp rootOp ) {
		final NullaryPhysicalOp pop = LogicalToPhysicalSamplingOpConverter.convert(rootOp);
		return createPlan(pop);
	}

	// --------- plans with unary root operators -----------
	/**
	 * Creates a plan with an index nested loops join as root operator.
	 */
	public static PhysicalPlan createPlanWithIndexNLJ( final LogicalOpGPAdd lop,
	                                                   final PhysicalPlan subplan ) {
		throw new UnsupportedOperationException("Not implemented yet");
//		final UnaryPhysicalOp pop = new PhysicalOpIndexNestedLoopsJoin(lop);
//		return createPlan(pop, subplan);
	}

	/**
	 * Creates a plan with a FILTER-based bind join as root operator.
	 */
	public static PhysicalPlan createPlanWithBindJoinFILTER( final LogicalOpGPAdd lop,
	                                                         final PhysicalPlan subplan ) {
		final UnaryPhysicalOp pop = new PhysicalOpFrawBindJoinWithFILTER(lop);
		return createPlan(pop, subplan);
	}

	/**
	 * Creates a plan with a UNION-based bind join as root operator.
	 */
	public static PhysicalPlan createPlanWithBindJoinUNION( final LogicalOpGPAdd lop,
	                                                        final PhysicalPlan subplan ) {
		final UnaryPhysicalOp pop = new PhysicalOpFrawBindJoinWithUNION(lop);
		return createPlan(pop, subplan);
	}

	/**
	 * Creates a plan with a VALUES-based bind join as root operator.
	 */
	public static PhysicalPlan createPlanWithBindJoinVALUES( final LogicalOpGPAdd lop,
	                                                         final PhysicalPlan subplan ) {
		final UnaryPhysicalOp pop = new PhysicalOpFrawBindJoinWithVALUES(lop);
		return createPlan(pop, subplan);
	}

	public static PhysicalPlan createPlanWithBindJoinVALUES( final LogicalOpGPOptAdd lop,
															 final PhysicalPlan subplan ) {
		final UnaryPhysicalOp pop = new PhysicalOpFrawBindJoinWithVALUES(lop);
		return createPlan(pop, subplan);
	}

	/**
	 * Creates a plan with a VALUES-based bind join that can switch to FILTER-based bind join as root operator.
	 */
	public static PhysicalPlan createPlanWithBindJoinVALUESorFILTER( final LogicalOpGPAdd lop,
	                                                                 final PhysicalPlan subplan ) {
		final UnaryPhysicalOp pop = new PhysicalOpFrawBindJoinWithVALUESorFILTER(lop);
		return createPlan(pop, subplan);
	}

	/**
	 * Creates a physical plan in which the root operator is the
	 * default physical operator for the given logical operator,
	 * as per {@link LogicalToPhysicalOpConverter}. The given
	 * subplan becomes the single child of the root operator.
	 */
	public static PhysicalPlan createPlan( final UnaryLogicalOp rootOp,
	                                       final PhysicalPlan subplan ) {
		final UnaryPhysicalOp pop = LogicalToPhysicalSamplingOpConverter.convert(rootOp);
		return createPlan(pop, subplan);
	}

	// --------- plans with binary root operators -----------

	/**
	 * Creates a plan with a binary union as root operator.
	 */
	public static PhysicalPlan createPlanWithUnion( final LogicalOpUnion lop,
	                                                final PhysicalPlan subplan1,
	                                                final PhysicalPlan subplan2 ) {
		final BinaryPhysicalOp pop = new PhysicalOpFrawBinaryUnion(lop);
		return createPlan(pop, subplan1, subplan2);
	}

	/**
	 * Creates a plan with a hash join as root operator.
	 */
	public static PhysicalPlan createPlanWithHashJoin( final LogicalOpJoin lop,
	                                                   final PhysicalPlan subplan1,
	                                                   final PhysicalPlan subplan2 ) {
		throw new UnsupportedOperationException("Not implemented yet");
//		final BinaryPhysicalOp pop = new PhysicalOpHashJoin(lop);
//		return createPlan(pop, subplan1, subplan2);
	}

	/**
	 * Creates a plan with a symmetric hash join as root operator.
	 */
	public static PhysicalPlan createPlanWithSymmetricHashJoin( final LogicalOpJoin lop,
	                                                            final PhysicalPlan subplan1,
	                                                            final PhysicalPlan subplan2 ) {
		throw new UnsupportedOperationException("Not implemented yet");
//		final BinaryPhysicalOp pop = new PhysicalOpSymmetricHashJoin(lop);
//		return createPlan(pop, subplan1, subplan2);
	}

	/**
	 * Creates a plan with a naive nested loops join as root operator.
	 */
	public static PhysicalPlan createPlanWithNaiveNLJ( final LogicalOpJoin lop,
	                                                   final PhysicalPlan subplan1,
	                                                   final PhysicalPlan subplan2 ) {
		throw new UnsupportedOperationException("Not implemented yet");
//		final BinaryPhysicalOp pop = new PhysicalOpNaiveNestedLoopsJoin(lop);
//		return createPlan(pop, subplan1, subplan2);
	}

	/**
	 * Creates a physical plan in which the root operator is the
	 * default physical operator for the given logical operator,
	 * as per {@link LogicalToPhysicalOpConverter}. The given
	 * subplans become children of the root operator.
	 */
	public static PhysicalPlan createPlan( final BinaryLogicalOp rootOp,
	                                       final PhysicalPlan subplan1,
	                                       final PhysicalPlan subplan2 ) {
		final BinaryPhysicalOp pop = LogicalToPhysicalSamplingOpConverter.convert(rootOp);
		return createPlan(pop, subplan1, subplan2);
	}


	// --------- plans with n-ary root operators -----------

	/**
	 * Creates a physical plan in which the root operator is the
	 * default physical operator for the given logical operator,
	 * as per {@link LogicalToPhysicalOpConverter}. The given
	 * subplans become children of the root operator.
	 */
	public static PhysicalPlan createPlan( final NaryLogicalOp rootOp,
	                                       final PhysicalPlan... subplans ) {
		final NaryPhysicalOp pop = LogicalToPhysicalSamplingOpConverter.convert(rootOp);
		return createPlan(pop, subplans);
	}

	/**
	 * Creates a physical plan in which the root operator is the
	 * default physical operator for the given logical operator,
	 * as per {@link LogicalToPhysicalOpConverter}. The given
	 * subplans become children of the root operator.
	 */
	public static PhysicalPlan createPlan( final NaryLogicalOp rootOp,
	                                       final List<PhysicalPlan> subplans ) {
		final NaryPhysicalOp pop = LogicalToPhysicalSamplingOpConverter.convert(rootOp);
		return createPlan(pop, subplans);
	}

	// --------- other special cases -----------

	/**
	 * This function take a inputPlan and unionPlan as input,
	 * where the unionPlan is required to be a plan with union as root operator, and all subPlans under the UNION are all requests or filters with request.
	 *
	 * In such cases, this function turns the requests under UNION into xxAdd operators with the inputPlan as subplans.
	 */
	public static PhysicalPlan createPlanWithUnaryOpForUnionPlan( final PhysicalPlan inputPlan, final PhysicalPlan unionPlan ) {
		final int numberOfSubPlansUnderUnion = unionPlan.numberOfSubPlans();
		final PhysicalPlan[] newUnionSubPlans = new PhysicalPlan[numberOfSubPlansUnderUnion];
		if ( numberOfSubPlansUnderUnion == 2 ) return createPlan( LogicalOpUnion.getInstance(), newUnionSubPlans );

		return PhysicalPlanFactory.createPlanWithUnaryOpForUnionPlan(inputPlan, unionPlan);
	}

	/**
	 * If the nextPlan is in the form of a request, filter with request, or union with requests,
	 * this function turns the requests into xxAdd operators with the inputPlan as subplans.
	 *
	 * Otherwise, it constructs a plan with a binary join between inputPlan and nextPlan (using the default physical operator)
	 **/
	public static PhysicalPlan createPlanWithDefaultUnaryOpIfPossible( final PhysicalPlan inputPlan, final PhysicalPlan nextPlan ) {
		final PhysicalOperator oldSubPlanRootOp = nextPlan.getRootOperator();
		if ( oldSubPlanRootOp instanceof PhysicalOpRequest ) {
			final PhysicalOpRequest<?,?> reqOp = (PhysicalOpRequest<?,?>) oldSubPlanRootOp;
			final UnaryLogicalOp addOp = LogicalOpUtils.createLogicalAddOpFromPhysicalReqOp(reqOp);
			return SamplingPhysicalPlanFactory.createPlan( addOp, inputPlan);
		}
		else if ( oldSubPlanRootOp instanceof PhysicalOpFilter
				&& nextPlan.getSubPlan(0).getRootOperator() instanceof PhysicalOpRequest ) {
			final PhysicalOpFilter filterOp = (PhysicalOpFilter) oldSubPlanRootOp;
			final PhysicalOpRequest<?,?> reqOp = (PhysicalOpRequest<?,?>) nextPlan.getSubPlan(0).getRootOperator();

			final UnaryLogicalOp addOp = LogicalOpUtils.createLogicalAddOpFromPhysicalReqOp(reqOp);
			final PhysicalPlan addOpPlan = SamplingPhysicalPlanFactory.createPlan( addOp, inputPlan);

			return SamplingPhysicalPlanFactory.createPlan( filterOp, addOpPlan);
		}
		else if ( oldSubPlanRootOp instanceof PhysicalOpLocalToGlobal
				&& nextPlan.getSubPlan(0).getRootOperator() instanceof PhysicalOpRequest ){
			final PhysicalOpLocalToGlobal l2gPOP = (PhysicalOpLocalToGlobal) oldSubPlanRootOp;
			final LogicalOpLocalToGlobal l2gLOP = (LogicalOpLocalToGlobal) l2gPOP.getLogicalOperator();
			final VocabularyMapping vm = l2gLOP.getVocabularyMapping();

			final LogicalOpGlobalToLocal g2l = new LogicalOpGlobalToLocal(vm);
			final PhysicalPlan newInputPlan = SamplingPhysicalPlanFactory.createPlan( new PhysicalOpGlobalToLocal(g2l), inputPlan );

			final PhysicalOpRequest<?,?> reqOp = (PhysicalOpRequest<?,?>) nextPlan.getSubPlan(0).getRootOperator();

			final UnaryLogicalOp addOp = LogicalOpUtils.createLogicalAddOpFromPhysicalReqOp(reqOp);
			final PhysicalPlan addOpPlan = SamplingPhysicalPlanFactory.createPlan( addOp, newInputPlan);

			return SamplingPhysicalPlanFactory.createPlan( l2gPOP, addOpPlan );
		}
		else if ( (oldSubPlanRootOp instanceof PhysicalOpFrawBinaryUnion || oldSubPlanRootOp instanceof PhysicalOpFrawMultiwayUnion)
				&& SamplingPhysicalPlanFactory.checkUnaryOpApplicableToUnionPlan(nextPlan)){
			
			return SamplingPhysicalPlanFactory.createPlanWithUnaryOpForUnionPlan( inputPlan, nextPlan );
		}
		else
			return createPlanWithJoin(inputPlan, nextPlan);
	}

	/**
	 * Check whether all operators under the UNION operator belong to any of the following:
	 * 	 - The operator is a request
	 * 	 - If the operator is a filter, then under that filter there must be a request,
	 * 	 - If the operator is a L2G operator, under the L2G operator, there must be a request or a filter operator with requests.
	 */
	public static boolean checkUnaryOpApplicableToUnionPlan( final PhysicalPlan unionPlan ){
		final PhysicalOperator rootOp = unionPlan.getRootOperator();
		if ( !(rootOp instanceof PhysicalOpFrawBinaryUnion || rootOp instanceof PhysicalOpFrawMultiwayUnion) ){
			return false;
		}

		for ( int i = 0; i < unionPlan.numberOfSubPlans(); i++ ) {
			final PhysicalPlan subPlan = unionPlan.getSubPlan(i);
			final PhysicalOperator subRootOp = subPlan.getRootOperator();
			if ( !(subRootOp instanceof PhysicalOpRequest || subRootOp instanceof PhysicalOpFilter || subRootOp instanceof PhysicalOpLocalToGlobal ) ) {
				return false;
			}

			if ( subRootOp instanceof PhysicalOpLocalToGlobal ){
				final PhysicalPlan subSubPlan = subPlan.getSubPlan(0);
				final PhysicalOperator subSubRootOp = subSubPlan.getRootOperator();
				if ( !( subSubRootOp instanceof PhysicalOpRequest || subSubRootOp instanceof PhysicalOpFilter) ){
					return false;
				}
				if ( subSubRootOp instanceof PhysicalOpFilter ){
					if ( !( subSubPlan.getSubPlan(0).getRootOperator() instanceof PhysicalOpRequest) ){
						return false;
					}
				}
			}

			if ( subRootOp instanceof PhysicalOpFilter ){
				if ( !( subPlan.getSubPlan(0).getRootOperator() instanceof PhysicalOpRequest) ){
					return false;
				}
			}
		}
		return true;
	}

}
