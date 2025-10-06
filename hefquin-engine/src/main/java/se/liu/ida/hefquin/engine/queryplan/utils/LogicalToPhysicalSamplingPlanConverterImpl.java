package se.liu.ida.hefquin.engine.queryplan.utils;

import se.liu.ida.hefquin.base.query.ExpectedVariables;
import se.liu.ida.hefquin.engine.queryplan.executable.NaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.engine.queryplan.logical.*;
import se.liu.ida.hefquin.engine.queryplan.logical.impl.*;
import se.liu.ida.hefquin.engine.queryplan.physical.*;
import se.liu.ida.hefquin.engine.queryplan.physical.impl.BaseForPhysicalOpMultiwayJoin;
import se.liu.ida.hefquin.engine.queryplan.physical.impl.PhysicalOpFrawRequest;
import se.liu.ida.hefquin.engine.queryplan.physical.impl.PhysicalOpParallelMultiLeftJoin;
import se.liu.ida.hefquin.engine.queryplan.physical.impl.PhysicalOpRequest;
import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.access.DataRetrievalRequest;

import java.util.*;

public class LogicalToPhysicalSamplingPlanConverterImpl implements LogicalToPhysicalPlanConverter
{
	protected final boolean ignorePhysicalOpsForLogicalAddOps;
	protected final boolean ignoreParallelMultiLeftJoin;

	public LogicalToPhysicalSamplingPlanConverterImpl(final boolean ignorePhysicalOpsForLogicalAddOps,
                                                      final boolean ignoreParallelMultiLeftJoin ) {
		this.ignorePhysicalOpsForLogicalAddOps = ignorePhysicalOpsForLogicalAddOps;
		this.ignoreParallelMultiLeftJoin = ignoreParallelMultiLeftJoin;
	}

	@Override
	public PhysicalPlan convert( final LogicalPlan lp, final boolean keepMultiwayJoins ) {
		return new Worker().convert(lp, keepMultiwayJoins);
	}

	// makes sure that sub-plans that are contained multiple times in the
	// given logical plan are converted only once; hence, the corresponding
	// physical sub-plan will then also contained multiple times within the
	// overall physical plan that is produced
	protected class Worker
	{
		final protected Map<LogicalPlan, PhysicalPlan> convertedSubPlans = new HashMap<>();

		public PhysicalPlan convert( final LogicalPlan lp, final boolean keepMultiwayJoins ) {
			final PhysicalPlan alreadyConverted = convertedSubPlans.get(lp);
			if ( alreadyConverted != null ) {
				return alreadyConverted;
			}

			final PhysicalPlan[] children = convertChildren(lp, keepMultiwayJoins);

			final QueryPlanningInfo qpInfo;
			if ( lp.hasQueryPlanningInfo() )
				qpInfo = lp.getQueryPlanningInfo();
			else
				qpInfo = null;

			final PhysicalPlan pp = createPhysicalPlan( lp.getRootOperator(), qpInfo, children, keepMultiwayJoins );
			convertedSubPlans.put(lp, pp);
			return pp;
		}

		/**
		 * Converts the sub-plans of the given logical plan (if any) and returns
		 * a list of the resulting physical plans. The order of the physical plans
		 * in the returned list is such that the i-th physical plan in the list
		 * is the physical plan that has been created for the i-th sub-plan of
		 * the given logical plan. For logical plans that do not contain any
		 * sub-plans, an empty list is returned.
		 */
		protected PhysicalPlan[] convertChildren( final LogicalPlan lp,
		                                              final boolean keepMultiwayJoins ) {
			final int numChildren = lp.numberOfSubPlans();
			final PhysicalPlan[] children = new PhysicalPlan[numChildren];

			for ( int i = 0; i < numChildren; ++i ) {
				children[i] = convert( lp.getSubPlan(i), keepMultiwayJoins );
			}

			return children;
		}

		protected PhysicalPlan createPhysicalPlan( final LogicalOperator lop,
												   final QueryPlanningInfo qpInfo,
		                                           final PhysicalPlan[] children,
		                                           final boolean keepMultiwayJoins )
		{
			if ( lop instanceof NullaryLogicalOp nullaryLOP ) {
				if ( children.length != 0 )
					throw new IllegalArgumentException( "unexpected number of sub-plans: " + children.length );

				return createPhysicalPlanWithNullaryRoot(nullaryLOP, qpInfo);
			}
			else if ( lop instanceof UnaryLogicalOp unaryLOP ) {
				if ( children.length != 1 )
					throw new IllegalArgumentException( "unexpected number of sub-plans: " + children.length );

				return createPhysicalPlanWithUnaryRoot( unaryLOP, qpInfo, children[0] );
			}
			else if ( lop instanceof BinaryLogicalOp binaryLOP ) {
				if ( children.length != 2 )
					throw new IllegalArgumentException( "unexpected number of sub-plans: " + children.length );

				return createPhysicalPlanWithBinaryRoot( binaryLOP, qpInfo, children[0], children[1] );
			}
			else if ( lop instanceof NaryLogicalOp naryLOP ) {

				if ( children.length < 1 )
					throw new IllegalArgumentException( "unexpected number of sub-plans: " + children.length );

				PhysicalPlan physicalPlan = createPhysicalPlanWithNaryRoot( naryLOP, qpInfo, children, keepMultiwayJoins );

				// Check if returned nary operator is a union of requests
				// If it is, transform into a random federation member request operator
				if(isNaryLogicalOpConvertibleIntoARandomRequestOperator(physicalPlan)){
					physicalPlan = unionWithRequestsToRandomRequestOperator(physicalPlan);
				}

				return physicalPlan;
			}
			else {
				throw new IllegalArgumentException( "unknown logical operator: " + lop.getClass().getName() );
			}
		}

		public static boolean isNaryLogicalOpConvertibleIntoARandomRequestOperator(final PhysicalPlan root){
			return _isNaryLogicalOpConvertibleIntoARandomRequestOperator(root, new HashSet<>());
		}

		public static boolean _isNaryLogicalOpConvertibleIntoARandomRequestOperator(final PhysicalPlan root, Set<DataRetrievalRequest> foundQueries){

			PhysicalOperator rootOp = root.getRootOperator();

			if(rootOp instanceof PhysicalOpRequest<?,?>){
				foundQueries.add(((PhysicalOpRequest) rootOp).getLogicalOperator().getRequest());
				return foundQueries.size() == 1;
			}

			if(!isMultiwayUnion(root.getRootOperator()) &&
					!isUnion(root.getRootOperator())) return false;


			for (int i = 0; i < root.numberOfSubPlans(); i++){
				PhysicalPlan subPlan = root.getSubPlan(i);

				if(!_isNaryLogicalOpConvertibleIntoARandomRequestOperator(subPlan, foundQueries)) return false;
			}

			return true;
		}

		public static PhysicalPlan unionWithRequestsToRandomRequestOperator(PhysicalPlan unionOfRequests) {
			Map<FederationMember, DataRetrievalRequest> memberToRequest = _unionWithRequestsToRandomRequestOperator(unionOfRequests);

			return PhysicalPlanFactory.createPlan(PhysicalOpFrawRequest.instantiate(memberToRequest));
		}

		public static Map<FederationMember, DataRetrievalRequest> _unionWithRequestsToRandomRequestOperator(PhysicalPlan unionOfRequests){
			if(unionOfRequests.getRootOperator() instanceof PhysicalOpRequest<?,?>) {
				LogicalOpRequest lop = ((PhysicalOpRequest) unionOfRequests.getRootOperator()).getLogicalOperator();
				return Map.of(lop.getFederationMember(), lop.getRequest());
			}

			Map<FederationMember, DataRetrievalRequest> reqMap = new HashMap<>();
			for (int i = 0; i < unionOfRequests.numberOfSubPlans(); i++){
				PhysicalPlan subPlan = unionOfRequests.getSubPlan(i);

				reqMap.putAll(_unionWithRequestsToRandomRequestOperator(subPlan));
			}

			return reqMap;
		}

		protected PhysicalPlan createPhysicalPlanWithNullaryRoot( final NullaryLogicalOp lop,
																  final QueryPlanningInfo qpInfo ) {
			return SamplingPhysicalPlanFactory.createPlan(lop, qpInfo);
		}

		protected PhysicalPlan createPhysicalPlanWithUnaryRoot( final UnaryLogicalOp lop,
																final QueryPlanningInfo qpInfo,
																final PhysicalPlan child ) {
			return SamplingPhysicalPlanFactory.createPlan(lop, qpInfo, child);
		}

		protected PhysicalPlan createPhysicalPlanWithBinaryRoot( final BinaryLogicalOp lop,
																 final QueryPlanningInfo qpInfo,
																 final PhysicalPlan child1,
																 final PhysicalPlan child2 ) {
			return SamplingPhysicalPlanFactory.createPlan(lop, qpInfo, child1, child2);
		}
		protected PhysicalPlan createPhysicalPlanWithNaryRoot( final NaryLogicalOp lop,
															   final QueryPlanningInfo qpInfo,
		                                                       final PhysicalPlan[] children,
		                                                       final boolean keepMultiwayJoins )
		{
			if ( lop instanceof LogicalOpMultiwayJoin mj )
				return createPhysicalPlanForMultiwayJoin( mj, qpInfo, children, keepMultiwayJoins );

			if ( lop instanceof LogicalOpMultiwayLeftJoin mlj )
				return createPhysicalPlanForMultiwayLeftJoin( mlj, qpInfo, children, keepMultiwayJoins );

			return SamplingPhysicalPlanFactory.createPlan(lop, qpInfo, children);
		}

		protected PhysicalPlan createPhysicalPlanForMultiwayJoin( final LogicalOpMultiwayJoin lop,
																  final QueryPlanningInfo qpInfo,
		                                                          final PhysicalPlan[] children,
		                                                          final boolean keepMultiwayJoins )
		{
			if ( children.length == 1 ) {
				return children[0];
			}

			if ( keepMultiwayJoins ) {
				final NaryPhysicalOp pop = new BaseForPhysicalOpMultiwayJoin(lop) {
					@Override public void visit(PhysicalPlanVisitor visitor) { throw new UnsupportedOperationException(); }
					@Override public NaryExecutableOp createExecOp(boolean collectExceptions, QueryPlanningInfo qpInfo, ExpectedVariables... inputVars) { throw new UnsupportedOperationException(); }
				};
				return SamplingPhysicalPlanFactory.createPlan(pop, qpInfo, children);
			}

			// Multiway joins are converted to a left-deep plan of joins, where
			// tpAdd and bgpAdd are used when possible; otherwise, binary joins
			// are used by default.
			PhysicalPlan currentSubPlan = children[0];
			for ( int i = 1; i < children.length; ++i ) {
				final PhysicalPlan nextChild = children[i];

				// If we are at the last subplan, which will end up becoming
				// the top of the left-deep plan constructed here, then we
				// can carry over the given qpInfo to that top plan. For all
				// intermediate steps of the left-deep plan we do not have
				// qpInfo objects.
				final QueryPlanningInfo qpInfoForSubPlan;
				if ( i == children.length - 1 )
					qpInfoForSubPlan = qpInfo;
				else
					qpInfoForSubPlan = null;

				if( ! ignorePhysicalOpsForLogicalAddOps ) {
					currentSubPlan = SamplingPhysicalPlanFactory.createPlanWithDefaultUnaryOpIfPossible(currentSubPlan, nextChild, qpInfoForSubPlan);
				}
				else {
					currentSubPlan = createPhysicalPlanWithBinaryRoot(
							LogicalOpJoin.getInstance(),
							qpInfoForSubPlan,
							currentSubPlan,
							nextChild );
				}
			}

			return currentSubPlan;
		}

		protected PhysicalPlan createPhysicalPlanForMultiwayLeftJoin( final LogicalOpMultiwayLeftJoin lop,
																	  final QueryPlanningInfo qpInfo,
		                                                              final PhysicalPlan[] children,
		                                                              final boolean keepMultiwayJoins )
		{
			if ( children.length == 1 ) {
				return children[0];
			}

			// Before going to the generic option that works for all cases,
			// check whether we have a case in which the parallel multi-left-
			// join can be used.
			if ( ! ignoreParallelMultiLeftJoin && children.length > 2 ) {
				final List<LogicalOpRequest<?,?>> optionalParts = PhysicalOpParallelMultiLeftJoin.checkApplicability(children);
				if ( optionalParts != null ) {
					// If the parallel multi-left-join can indeed be used, do so.
					final UnaryPhysicalOp rootOp = new PhysicalOpParallelMultiLeftJoin(optionalParts);
					return SamplingPhysicalPlanFactory.createPlan( rootOp, children[0] );
				}
			}

			// Now comes the generic option that works for all cases.

			// Multiway left joins are converted to right-deep plans of right
			// outer joins, where tpOptAdd and bgpOptAdd are used when possible;
			// otherwise, binary outer joins are used by default.

			// The first child of the multiway left join is the non-optional part
			// and, thus, is used as the right input to the first right outer join
			// (second case below) or as the input to the tpOptAdd/bgpOptAdd (first
			// case below).
			PhysicalPlan currentSubPlan = children[0];
			for ( int i = 1; i < children.length; ++i ) {
				final PhysicalPlan nextChild = children[i];
				final PhysicalOperator rootOpOfNextChild = nextChild.getRootOperator();
				if( ! ignorePhysicalOpsForLogicalAddOps && rootOpOfNextChild instanceof PhysicalOpRequest ){
					currentSubPlan = createPhysicalPlanWithUnaryRoot(
							LogicalOpUtils.createLogicalOptAddOpFromPhysicalReqOp(rootOpOfNextChild),
							null,
							currentSubPlan );
				}
				else {
					currentSubPlan = createPhysicalPlanWithBinaryRoot(
							LogicalOpRightJoin.getInstance(),
							null,
							nextChild,
							currentSubPlan );
				}
			}

			return currentSubPlan;
		}
	} // end of helper class Worker

	public static boolean isUnion( final PhysicalOperator op ) {
		final LogicalOperator lop = ((PhysicalOperatorForLogicalOperator) op).getLogicalOperator();
		return lop instanceof LogicalOpUnion;
	}
	
	public static boolean isMultiwayUnion( final PhysicalOperator op ) {
		final LogicalOperator lop = ((PhysicalOperatorForLogicalOperator) op).getLogicalOperator();
		return lop instanceof LogicalOpMultiwayUnion;
	}
}
