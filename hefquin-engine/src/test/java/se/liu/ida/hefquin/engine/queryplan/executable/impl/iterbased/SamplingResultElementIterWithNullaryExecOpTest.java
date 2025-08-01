package se.liu.ida.hefquin.engine.queryplan.executable.impl.iterbased;

import org.junit.Test;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.ExecutableOperatorStats;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.NullaryExecutableOp;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.ops.BaseForExecOps;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SamplingResultElementIterWithNullaryExecOpTest
{

	private static SolutionMapping EMPTY = new SolutionMappingImpl();

	@Test
	public void getOpTest() {
		final NullaryExecutableOpForTest op = new NullaryExecutableOpForTest();
		final ResultElementIterWithNullaryExecOp it = new ResultElementIterWithNullaryExecOp( op, TestUtils.createExecContextForTests() );

		assertEquals( op, it.getOp() );
	}

	@Test
	public void nextWithEmptyIterator() {
		final ResultElementIterator it = createIteratorForTests();

		assertEquals( it.hasNext(), true );
		assertEquals( it.next(), EMPTY );
	}

	@Test
	public void nextWithHasNext() {
		final SolutionMapping sm1 = TestUtils.createSolutionMappingForTests();
		final SolutionMapping sm2 = TestUtils.createSolutionMappingForTests();
		final SolutionMapping sm3 = TestUtils.createSolutionMappingForTests();
		final ResultElementIterator it = createIteratorForTests( sm1, sm2, sm3 );

		assertTrue( it.hasNext() );
		assertEquals( sm1, it.next() );
		assertTrue( it.hasNext() );
		assertEquals( sm2, it.next() );
		assertTrue( it.hasNext() );
		assertEquals( sm3, it.next() );

		assertEquals( it.hasNext(), true );
		assertEquals( it.next(), sm1 );
	}

	@Test
	public void testCache(){
		final SolutionMapping sm1 = TestUtils.createSolutionMappingForTests("1");
		final SolutionMapping sm2 = TestUtils.createSolutionMappingForTests("2");
		final SolutionMapping sm3 = TestUtils.createSolutionMappingForTests("3");

		StatsProvidingResultElementIterator it = createIteratorForTests( sm1, sm2, sm3 );

		for ( int i = 0; i < 10; i++ ) {
			it.next();
		}

		assertEquals( it.getNumberOfNexts(), 10 );

		// 1 for startup -> results 1, 2, 3
		// 2 -> results 4, 5, 6
		// 3 -> results 7, 8, 9
		// 4 -> results 10
		assertEquals( it.getNumberOfThreadWakeUps(), 4 );


	}


	protected static StatsProvidingResultElementIterator createIteratorForTests( SolutionMapping... elements ) {
		return new SamplingResultElementIterWithNullaryExecOp(
						new NullaryExecutableOpForTest(elements),
						TestUtils.createExecContextForTests() );
	}

	protected static class NullaryExecutableOpForTest extends BaseForExecOps implements NullaryExecutableOp
	{
		final List<SolutionMapping> list;

		public NullaryExecutableOpForTest() {
			super(false);
			list = null;
		}

		public NullaryExecutableOpForTest( final SolutionMapping[] elements ) {
			super(false);
			list = Arrays.asList(elements);
		}

		@Override
		public void execute( final IntermediateResultElementSink sink,
		                     final ExecutionContext execCxt )
		{
			if ( list != null ) {
				for ( final SolutionMapping sm : list ) {
					sink.send(sm);
				}
			}
		}

		@Override
		public void resetStats() {
		}

		@Override
		public ExecutableOperatorStats getStats() {
			return null;
		}
	}



}
