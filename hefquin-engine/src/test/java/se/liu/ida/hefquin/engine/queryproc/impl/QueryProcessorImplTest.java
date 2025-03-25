package se.liu.ida.hefquin.engine.queryproc.impl;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Test;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.query.Query;
import se.liu.ida.hefquin.base.query.impl.GenericSPARQLGraphPatternImpl1;
import se.liu.ida.hefquin.engine.EngineTestBase;
import se.liu.ida.hefquin.engine.HeFQUINEngine;
import se.liu.ida.hefquin.engine.HeFQUINEngineDefaultComponents;
import se.liu.ida.hefquin.engine.HeFQUINEngineImpl;
import se.liu.ida.hefquin.engine.federation.BRTPFServer;
import se.liu.ida.hefquin.engine.federation.TPFServer;
import se.liu.ida.hefquin.engine.federation.access.BRTPFRequest;
import se.liu.ida.hefquin.engine.federation.access.FederationAccessManager;
import se.liu.ida.hefquin.engine.federation.access.TPFRequest;
import se.liu.ida.hefquin.engine.federation.access.TPFResponse;
import se.liu.ida.hefquin.engine.federation.access.impl.BlockingFederationAccessManagerImpl;
import se.liu.ida.hefquin.engine.federation.access.impl.reqproc.*;
import se.liu.ida.hefquin.engine.federation.catalog.FederationCatalog;
import se.liu.ida.hefquin.engine.federation.catalog.impl.FederationCatalogImpl;
import se.liu.ida.hefquin.engine.queryplan.logical.LogicalPlan;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalToPhysicalPlanConverter;
import se.liu.ida.hefquin.engine.queryproc.*;
import se.liu.ida.hefquin.engine.queryproc.impl.compiler.IteratorBasedSamplingQueryPlanCompilerImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.compiler.PushBasedQueryPlanCompilerImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.execution.ExecutionEngineImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.execution.FrawExecutionEngineImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.planning.QueryPlannerImpl;
import se.liu.ida.hefquin.engine.queryproc.impl.poptimizer.PhysicalOptimizerWithoutOptimization;
import se.liu.ida.hefquin.engine.queryproc.impl.srcsel.ServiceClauseBasedSourcePlannerImpl;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class QueryProcessorImplTest extends EngineTestBase
{
	@Test
	public void oneTPFoneTriplePattern() throws QueryProcException {
		// setting up
		final String queryString = "SELECT * WHERE {"
				+ "SERVICE <http://example.org> { ?x <http://example.org/p> ?y }"
				+ "}";

		final Graph dataForMember = GraphFactory.createGraphMem();
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://example.org/s"),
				NodeFactory.createURI("http://example.org/p"),
				NodeFactory.createURI("http://example.org/o")) );
		
		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
		fedCat.addMember( "http://example.org", new TPFServerForTest(dataForMember) );

		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();

		final Iterator<SolutionMapping> it = processQuery(queryString, fedCat, fedAccessMgr);

		// checking
		assertTrue( it.hasNext() );

		final Binding sm1 = it.next().asJenaBinding();
		assertEquals( 2, sm1.size() );
		final Var varX = Var.alloc("x");
		final Var varY = Var.alloc("y");
		assertTrue( sm1.contains(varX) );
		assertTrue( sm1.contains(varY) );
		assertEquals( "http://example.org/s", sm1.get(varX).getURI() );
		assertEquals( "http://example.org/o", sm1.get(varY).getURI() );

		assertFalse( it.hasNext() );
	}

	@Test
	public void oneBRTPFtwoTriplePatterns() throws QueryProcException {
		// setting up
		final String queryString = "SELECT * WHERE {"
				+ "SERVICE <http://example.org> { ?x <http://example.org/p1> ?y; <http://example.org/p2> ?z }"
				+ "}";

		final Graph dataForMember = GraphFactory.createGraphMem();
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://example.org/s"),
				NodeFactory.createURI("http://example.org/p1"),
				NodeFactory.createURI("http://example.org/o1")) );
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://example.org/s"),
				NodeFactory.createURI("http://example.org/p2"),
				NodeFactory.createURI("http://example.org/o2")) );

		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
		fedCat.addMember( "http://example.org", new BRTPFServerForTest(dataForMember) );

		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();

		final Iterator<SolutionMapping> it = processQuery(queryString, fedCat, fedAccessMgr);

		// checking
		assertTrue( it.hasNext() );

		final Binding sm1 = it.next().asJenaBinding();
		assertEquals( 3, sm1.size() );
		final Var varX = Var.alloc("x");
		final Var varY = Var.alloc("y");
		final Var varZ = Var.alloc("z");
		assertTrue( sm1.contains(varX) );
		assertTrue( sm1.contains(varY) );
		assertTrue( sm1.contains(varZ) );
		assertEquals( "http://example.org/s", sm1.get(varX).getURI() );
		assertEquals( "http://example.org/o1", sm1.get(varY).getURI() );
		assertEquals( "http://example.org/o2", sm1.get(varZ).getURI() );

		assertFalse( it.hasNext() );
	}

	@Test
	public void twoTPFtwoTriplePatterns() throws QueryProcException {
		// setting up
		final String queryString = "SELECT * WHERE {"
				+ "SERVICE <http://example.org/tpf1> { ?x <http://example.org/p1> ?y }"
				+ "SERVICE <http://example.org/tpf2> { ?x <http://example.org/p2> ?z }"
				+ "}";

		final Graph dataForMember1 = GraphFactory.createGraphMem();
		dataForMember1.add( Triple.create(
				NodeFactory.createURI("http://example.org/s"),
				NodeFactory.createURI("http://example.org/p1"),
				NodeFactory.createURI("http://example.org/o1")) );

		final Graph dataForMember2 = GraphFactory.createGraphMem();
		dataForMember2.add( Triple.create(
				NodeFactory.createURI("http://example.org/s"),
				NodeFactory.createURI("http://example.org/p2"),
				NodeFactory.createURI("http://example.org/o2")) );
		
		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
		fedCat.addMember( "http://example.org/tpf1", new TPFServerForTest(dataForMember1) );
		fedCat.addMember( "http://example.org/tpf2", new TPFServerForTest(dataForMember2) );

		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();

		final Iterator<SolutionMapping> it = processQuery(queryString, fedCat, fedAccessMgr);

		// checking
		assertTrue( it.hasNext() );

		final Binding sm1 = it.next().asJenaBinding();
		assertEquals( 3, sm1.size() );
		final Var varX = Var.alloc("x");
		final Var varY = Var.alloc("y");
		final Var varZ = Var.alloc("z");
		assertTrue( sm1.contains(varX) );
		assertTrue( sm1.contains(varY) );
		assertTrue( sm1.contains(varZ) );
		assertEquals( "http://example.org/s", sm1.get(varX).getURI() );
		assertEquals( "http://example.org/o1", sm1.get(varY).getURI() );
		assertEquals( "http://example.org/o2", sm1.get(varZ).getURI() );

		assertFalse( it.hasNext() );
	}

	@Test
	public void unionOverTwoTriplePatterns() throws QueryProcException {
		// setting up
		final String queryString = "SELECT * WHERE {"
				+ "{ SERVICE <http://example.org/tpf1> { ?x <http://example.org/p1> ?y } }"
				+ "UNION"
				+ "{ SERVICE <http://example.org/tpf2> { ?x <http://example.org/p1> ?y } }"
				+ "}";

		final Graph dataForMember1 = GraphFactory.createGraphMem();
		dataForMember1.add( Triple.create(
				NodeFactory.createURI("http://example.org/s"),
				NodeFactory.createURI("http://example.org/p1"),
				NodeFactory.createURI("http://example.org/o1")) );

		final Graph dataForMember2 = GraphFactory.createGraphMem();
		dataForMember2.add( Triple.create(
				NodeFactory.createURI("http://example.org/s"),
				NodeFactory.createURI("http://example.org/p1"),
				NodeFactory.createURI("http://example.org/o2")) );
		
		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
		fedCat.addMember( "http://example.org/tpf1", new TPFServerForTest(dataForMember1) );
		fedCat.addMember( "http://example.org/tpf2", new TPFServerForTest(dataForMember2) );

		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();

		final Iterator<SolutionMapping> it = processQuery(queryString, fedCat, fedAccessMgr);

		// checking
		assertTrue( it.hasNext() );

		final Binding sm1 = it.next().asJenaBinding();
		assertEquals( 2, sm1.size() );
		final Var varX = Var.alloc("x");
		final Var varY = Var.alloc("y");
		assertTrue( sm1.contains(varX) );
		assertTrue( sm1.contains(varY) );
		assertEquals( "http://example.org/s", sm1.get(varX).getURI() );
		final String uriY1 = sm1.get(varY).getURI();
		assertTrue( uriY1.equals("http://example.org/o1") || uriY1.equals("http://example.org/o2") );

		assertTrue( it.hasNext() );

		final Binding sm2 = it.next().asJenaBinding();
		assertEquals( 2, sm2.size() );
		assertTrue( sm2.contains(varX) );
		assertTrue( sm2.contains(varY) );
		assertEquals( "http://example.org/s", sm2.get(varX).getURI() );
		final String uriY2 = sm2.get(varY).getURI();
		assertTrue( uriY2.equals("http://example.org/o1") || uriY2.equals("http://example.org/o2") );

		assertFalse( it.hasNext() );
	}

	@Test
	public void oneTPFoneTriplePatternWithFilterInside() throws QueryProcException {
		// setting up
		final String queryString = "SELECT * WHERE { "
				+ "SERVICE <http://example.org> { ?x <http://example.org/p> ?y FILTER (?y = <http://example.org/o1>) } "
				+ "}";

		final Graph dataForMember = GraphFactory.createGraphMem();
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://example.org/s1"),
				NodeFactory.createURI("http://example.org/p"),
				NodeFactory.createURI("http://example.org/o1")) );
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://example.org/s2"),
				NodeFactory.createURI("http://example.org/p"),
				NodeFactory.createURI("http://example.org/o2")) );
		
		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
		fedCat.addMember( "http://example.org", new TPFServerForTest(dataForMember) );

		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();

		final Iterator<SolutionMapping> it = processQuery(queryString, fedCat, fedAccessMgr);

		// checking
		assertTrue( it.hasNext() );

		final Binding sm1 = it.next().asJenaBinding();
		assertEquals( 2, sm1.size() );
		final Var varX = Var.alloc("x");
		final Var varY = Var.alloc("y");
		assertTrue( sm1.contains(varX) );
		assertTrue( sm1.contains(varY) );
		assertEquals( "http://example.org/s1", sm1.get(varX).getURI() );
		assertEquals( "http://example.org/o1", sm1.get(varY).getURI() );

		assertFalse( it.hasNext() );
	}

	@Test
	public void oneTPFoneTriplePatternWithFilterOutside() throws QueryProcException {
		// setting up
		final String queryString = "SELECT * WHERE { "
				+ "SERVICE <http://example.org> { ?x <http://example.org/p> ?y } "
				+ "FILTER (?y = <http://example.org/o1>) "
				+ "}";

		final Graph dataForMember = GraphFactory.createGraphMem();
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://example.org/s1"),
				NodeFactory.createURI("http://example.org/p"),
				NodeFactory.createURI("http://example.org/o1")) );
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://example.org/s2"),
				NodeFactory.createURI("http://example.org/p"),
				NodeFactory.createURI("http://example.org/o2")) );
		
		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
		fedCat.addMember( "http://example.org", new TPFServerForTest(dataForMember) );

		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();

		final Iterator<SolutionMapping> it = processQuery(queryString, fedCat, fedAccessMgr);

		// checking
		assertTrue( it.hasNext() );

		final Binding sm1 = it.next().asJenaBinding();
		assertEquals( 2, sm1.size() );
		final Var varX = Var.alloc("x");
		final Var varY = Var.alloc("y");
		assertTrue( sm1.contains(varX) );
		assertTrue( sm1.contains(varY) );
		assertEquals( "http://example.org/s1", sm1.get(varX).getURI() );
		assertEquals( "http://example.org/o1", sm1.get(varY).getURI() );

		assertFalse( it.hasNext() );
	}

	@Test
	public void liveTestWithDBpedia() throws QueryProcException {
		if ( ! skipLiveWebTests ) {
			// setting up
			final String dbpediaURL = "http://dbpedia.org/sparql";
			final String queryString = "SELECT * WHERE {"
					+ "SERVICE <" + dbpediaURL + "> { <http://dbpedia.org/resource/Berlin> <http://xmlns.com/foaf/0.1/name> ?o }"
					+ "}";

			final FederationCatalogImpl fedCat = new FederationCatalogImpl();
			fedCat.addMember( dbpediaURL, new SPARQLEndpointForTest(dbpediaURL) );

			final SPARQLRequestProcessor reqProcSPARQL = new SPARQLRequestProcessorImpl();
			final TPFRequestProcessor reqProcTPF = new TPFRequestProcessor() {
				@Override public TPFResponse performRequest(TPFRequest req, TPFServer fm) { return null; }
				@Override public TPFResponse performRequest(TPFRequest req, BRTPFServer fm) { return null; }
			};
			final BRTPFRequestProcessor reqProcBRTPF = new BRTPFRequestProcessor() {
				@Override public TPFResponse performRequest(BRTPFRequest req, BRTPFServer fm) { return null; }
			};
			final Neo4jRequestProcessor reqProcNeo4j = new Neo4jRequestProcessorImpl();
			final FederationAccessManager fedAccessMgr = new BlockingFederationAccessManagerImpl(reqProcSPARQL, reqProcTPF, reqProcBRTPF, reqProcNeo4j);

			// executing the tested method
			final Iterator<SolutionMapping> it = processQuery(queryString, fedCat, fedAccessMgr);

			// checking
			assertTrue( it.hasNext() );

			final Binding b = it.next().asJenaBinding();
			final Var var = Var.alloc("o");
			assertEquals( 1, b.size() );
			assertTrue( b.contains(var) );

			final Node n = b.get(var);
			assertTrue( n.isLiteral() );
		}
	}


	protected Iterator<SolutionMapping> processQuery( final String queryString,
	                                                  final FederationCatalog fedCat,
	                                                  final FederationAccessManager fedAccessMgr ) throws QueryProcException {
		final ExecutorService execServiceForPlanTasks = HeFQUINEngineDefaultComponents.createExecutorServiceForPlanTasks();
		final LogicalToPhysicalPlanConverter l2pConverter = HeFQUINEngineDefaultComponents.createDefaultLogicalToPhysicalPlanConverter();

		final QueryProcContext ctxt = new QueryProcContext() {
			@Override public FederationCatalog getFederationCatalog() { return fedCat; }
			@Override public FederationAccessManager getFederationAccessMgr() { return fedAccessMgr; }
			@Override public ExecutorService getExecutorServiceForPlanTasks() { return execServiceForPlanTasks; }
			@Override public boolean isExperimentRun() { return false; }
			@Override public boolean skipExecution() { return false; }
		};

		final SourcePlanner sourcePlanner = new ServiceClauseBasedSourcePlannerImpl(ctxt);

		final LogicalOptimizer loptimizer = new LogicalOptimizer() {
			@Override
			public LogicalPlan optimize( final LogicalPlan p, final boolean keepNaryOperators ) {
				return p;
			}
		};

		final PhysicalOptimizer poptimizer = new PhysicalOptimizerWithoutOptimization(l2pConverter);
		final QueryPlanner planner = new QueryPlannerImpl(sourcePlanner, loptimizer, poptimizer, null, null, null);
		final QueryPlanCompiler planCompiler = new
				//IteratorBasedQueryPlanCompilerImpl(ctxt);
				//PullBasedQueryPlanCompilerImpl(ctxt);
				PushBasedQueryPlanCompilerImpl(ctxt);
		final ExecutionEngine execEngine = new ExecutionEngineImpl();
		final QueryProcessor qProc = new QueryProcessorImpl(planner, planCompiler, execEngine, ctxt);
		final MaterializingQueryResultSinkImpl resultSink = new MaterializingQueryResultSinkImpl();
		// Doesn't work with group bys, because of the getQueryPattern call
		final Query query = new GenericSPARQLGraphPatternImpl1( QueryFactory.create(queryString).getQueryPattern() );

		qProc.processQuery(query, resultSink);

		execServiceForPlanTasks.shutdownNow();
		try {
			execServiceForPlanTasks.awaitTermination(500L, TimeUnit.MILLISECONDS);
		}
		catch ( final InterruptedException ex )  {
System.err.println("Terminating the thread pool was interrupted." );
			ex.printStackTrace();
		}

		return resultSink.getSolMapsIter();
	}


	// -------------------------------------- HEFQUIN FRAW TESTS -------------------------------------------------------

	String spo = "SELECT * WHERE { SERVICE <http://example.org> { ?s ?p ?o } }";

	String noresult = "SELECT * WHERE { ?s ?p \"doesntExist\" }";

	String groupBy = "SELECT ?s (COUNT(?p) as ?nbPredicate) WHERE { SERVICE <http://example.org> { ?s ?p ?o } } GROUP BY ?s";

	String nestedGroupBy =
					"SELECT ?person (count(?species) as ?numberOfSpecies) WHERE {\n" +
					"    {\n" +
					"        SELECT ?person ?species WHERE {\n" +
					"            SERVICE <http://example.org> {\n" +
					"				?person <http://own> ?animal.\n" +
					"           	?animal <http://species> ?species.\n" +
					"			}\n" +
					"        }\n" +
					"        group by ?person ?species\n" +
					"    }\n" +
					"}\n" +
					"group by ?person";

	String joinGroupByAsInput =
			"SELECT ?person (count(?species) as ?numberOfSpecies) WHERE {\n" +
					"    {\n" +
					"        SELECT ?person ?species WHERE {\n" +
					"            SERVICE <http://example.org> {\n" +
					"				?person <http://own> ?animal.\n" +
					"           	?animal <http://species> ?species.\n" +
					"			}\n" +
					"        }\n" +
					"        group by ?person ?species\n" +
					"    }.\n" +
					"    ?person <http://own> ?animal. " +
					"}\n" +
					"group by ?person";

	String joinGroupByWithInput =
			"SELECT ?person (count(?species) as ?numberOfSpecies) WHERE {\n" +
					"    ?person <http://own> ?animal." +
					"    {\n" +
					"        SELECT ?person ?species WHERE {\n" +
					"            SERVICE <http://example.org> {\n" +
					"				?person <http://own> ?animal.\n" +
					"           	?animal <http://species> ?species.\n" +
					"			}\n" +
					"        }\n" +
					"        group by ?person ?species\n" +
					"    }.\n" +
					"}\n" +
					"group by ?person";


	private static Graph getGraph(){
		Graph dataForMember = GraphFactory.createGraphMem();
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://Alice"),
				NodeFactory.createURI("http://address"),
				NodeFactory.createURI("http://nantes")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://Bob"),
				NodeFactory.createURI("http://address"),
				NodeFactory.createURI("http://paris")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://Carole"),
				NodeFactory.createURI("http://address"),
				NodeFactory.createURI("http://nantes")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://Alice"),
				NodeFactory.createURI("http://own"),
				NodeFactory.createURI("http://cat")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://Alice"),
				NodeFactory.createURI("http://own"),
				NodeFactory.createURI("http://bird")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://Bob"),
				NodeFactory.createURI("http://own"),
				NodeFactory.createURI("http://dog")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://Carole"),
				NodeFactory.createURI("http://own"),
				NodeFactory.createURI("http://snake")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://cat"),
				NodeFactory.createURI("http://species"),
				NodeFactory.createURI("http://feline")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://bird"),
				NodeFactory.createURI("http://species"),
				NodeFactory.createURI("http://birb")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://dog"),
				NodeFactory.createURI("http://species"),
				NodeFactory.createURI("http://canine")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://snake"),
				NodeFactory.createURI("http://species"),
				NodeFactory.createURI("http://reptile")) );

		return dataForMember;
	}


	@Test
	public void testGroupBy() throws QueryProcException, UnsupportedEncodingException {

//		final DatasetGraph dsg = DatasetGraphFactory.createGeneral();
//		dsg.addGraph(NodeFactory.createURI("http://example.com"), getGraph());
//		final QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(spo), dsg);


		final Graph dataForMember = getGraph();

		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
		fedCat.addMember("http://example.org" , new SPARQLEndpointForTest(dataForMember) );

		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();


		final ExecutorService execServiceForPlanTasks = HeFQUINEngineDefaultComponents.createExecutorServiceForPlanTasks();
		final LogicalToPhysicalPlanConverter l2pConverter = HeFQUINEngineDefaultComponents.createDefaultLogicalToPhysicalPlanConverter();

		final QueryProcContext ctxt = new QueryProcContext() {
			@Override public FederationCatalog getFederationCatalog() { return fedCat; }
			@Override public FederationAccessManager getFederationAccessMgr() { return fedAccessMgr; }
			@Override public ExecutorService getExecutorServiceForPlanTasks() { return execServiceForPlanTasks; }
			@Override public boolean isExperimentRun() { return false; }
			@Override public boolean skipExecution() { return false; }
		};

		final SourcePlanner sourcePlanner = new ServiceClauseBasedSourcePlannerImpl(ctxt);

		final LogicalOptimizer loptimizer = (p, keepNaryOperators) -> p;
		final PhysicalOptimizer poptimizer = new PhysicalOptimizerWithoutOptimization(l2pConverter);

		final QueryPlanner planner = new QueryPlannerImpl(sourcePlanner, loptimizer, poptimizer, null, null, null);
		final QueryPlanCompiler planCompiler = new IteratorBasedSamplingQueryPlanCompilerImpl(ctxt);
		final ExecutionEngine execEngine = new FrawExecutionEngineImpl();

		final QueryProcessor qProc = new QueryProcessorImpl(planner, planCompiler, execEngine, ctxt);

		HeFQUINEngine engine = new HeFQUINEngineImpl(fedAccessMgr, qProc);

		engine.integrateIntoJena();

		engine.executeQuery(QueryFactory.create(nestedGroupBy));


//		final Iterator<SolutionMapping> it = processQuery(groupBy, fedCat, fedAccessMgr);
//
//		while ( it.hasNext() ){
//			final SolutionMapping sol = it.next();
//			System.out.println(sol.toString());
//		}
	}
}
