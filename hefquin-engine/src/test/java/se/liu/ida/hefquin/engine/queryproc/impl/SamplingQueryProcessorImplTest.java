package se.liu.ida.hefquin.engine.queryproc.impl;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.QC;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Assert;
import org.junit.Test;
import se.liu.ida.hefquin.engine.EngineTestBase;
import se.liu.ida.hefquin.engine.queryproc.QueryProcException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

public class SamplingQueryProcessorImplTest extends EngineTestBase
{

	String spo = "SELECT * WHERE { SERVICE <http://example.org> { ?s ?p ?o } }";

	String spop1o1 = "SELECT * WHERE { SERVICE <http://example.org> { ?s ?p ?o. } . SERVICE <http://example2.org> { ?o ?p1 ?o1. } }";

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
			"SELECT * WHERE {\n" +
					"    {\n" +
					"        SELECT ?person ?species WHERE {\n" +
					"            SERVICE <http://example.org> {\n" +
					"				?person <http://own> ?animal.\n" +
					"           	?animal <http://species> ?species.\n" +
					"			}\n" +
					"        }\n" +
					"        group by ?person ?species\n" +
					"    }.\n" +
					"    SERVICE <http://example.org> {\n" +
					"    	?person <http://own> ?animal. " +
					"    }.\n" +
					"}\n";

	String joinGroupByWithInput =
			"SELECT * WHERE {\n" +
					"    SERVICE <http://example.org> {\n" +
					"    	?person <http://own> ?animal. " +
					"    }.\n" +
					"    {\n" +
					"        SELECT ?person ?species WHERE {\n" +
					"            SERVICE <http://example.org> {\n" +
					"				?person <http://own> ?animal.\n" +
					"           	?animal <http://species> ?species.\n" +
					"			}\n" +
					"        }\n" +
					"        group by ?person ?species\n" +
					"    }.\n" +
					"}\n";


	private static Graph getGraph2(){
		Graph dataForMember = GraphFactory.createGraphMem();
		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://cat"),
				NodeFactory.createURI("http://loves"),
				NodeFactory.createURI("http://food")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://dog"),
				NodeFactory.createURI("http://loves"),
				NodeFactory.createURI("http://bone")) );

		dataForMember.add( Triple.create(
				NodeFactory.createURI("http://bird"),
				NodeFactory.createURI("http://loves"),
				NodeFactory.createURI("http://dance")) );

		return dataForMember;
	}

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

	private ResultSet runQuery(String queryString) {
		// TODO: get this working again
		return null;
//		final Graph dataForMember = getGraph();
//		final Graph dataForMember2 = getGraph2();
//
//		final DatasetGraph dsg = DatasetGraphFactory.createGeneral();
//		dsg.addGraph(NodeFactory.createURI("http://example.com"), dataForMember);
//		final QueryExecution qe = QueryExecutionFactory.create(QueryFactory.create(queryString), dsg);
//
//
//
//		final FederationCatalogImpl fedCat = new FederationCatalogImpl();
//		fedCat.addMember("http://example.org" , new SPARQLEndpointForTest(dataForMember) );
//		fedCat.addMember("http://example2.org" , new SPARQLEndpointForTest(dataForMember2) );
//
//		final FederationAccessManager fedAccessMgr = new FederationAccessManagerForTest();
//
//		final ExecutorService execServiceForPlanTasks = Executors.newFixedThreadPool(10);
//		final LogicalToPhysicalPlanConverter l2pConverter = new LogicalToPhysicalSamplingPlanConverterImpl(false, false);
//
//		final QueryProcContext ctxt = new QueryProcContext() {
//			@Override public FederationCatalog getFederationCatalog() { return fedCat; }
//			@Override public FederationAccessManager getFederationAccessMgr() { return fedAccessMgr; }
//			@Override public ExecutorService getExecutorServiceForPlanTasks() { return execServiceForPlanTasks; }
//			@Override public boolean isExperimentRun() { return false; }
//			@Override public boolean skipExecution() { return false; }
//		};
//
//		final SourcePlanner sourcePlanner = new ServiceClauseBasedSourcePlannerImpl(ctxt);
//
//		final LogicalOptimizer loptimizer = (p, keepNaryOperators) -> p;
//		final PhysicalOptimizer poptimizer = new PhysicalOptimizerWithoutOptimization(l2pConverter);
//
//		final QueryPlanner planner = new QueryPlannerImpl(sourcePlanner, loptimizer, poptimizer, null, null, null);
//		final SamplingQueryPlanCompiler planCompiler = new IteratorBasedSamplingQueryPlanCompilerImpl(ctxt);
//		final ExecutionEngine execEngine = new FrawExecutionEngineImpl();
//
//		final QueryProcessor qProc = new SamplingQueryProcessorImpl(planner, planCompiler, execEngine, ctxt);
//
//		HeFQUINEngine engine = new FrawEngine(fedAccessMgr, qProc, 100, 1);
//
//		engine.integrateIntoJena();
//
//		final ResultSet rs;
//		try {
//			rs = qe.execSelect();
//			return rs;
//		}
//		catch ( final Exception e ) {
//			return null;
//		}
	}

	private QueryIterator runQueryWithJena(String queryString) {

		QC.setFactory( ARQ.getContext(), (execCxt -> OpExecutor.stdFactory.create(execCxt)) );
		final QueryIterator qIter = Algebra.exec(Algebra.compile(QueryFactory.create(queryString)), getGraph());

		return qIter;
	}

	@Test
	public void testSPO() throws QueryProcException, UnsupportedEncodingException {

		ResultSet rs = runQuery(spo);
		List<QuerySolution> results = new ArrayList<>();

		while(rs.hasNext()) {
			results.add(rs.next());
		}

		Assert.assertTrue(results.get(0).contains("s"));
		Assert.assertTrue(results.get(0).contains("p"));
		Assert.assertTrue(results.get(0).contains("o"));
	}

	@Test
	public void testSPOP1O1() throws QueryProcException, UnsupportedEncodingException {
		// dependent on data and join ordering as well

		ResultSet rs = runQuery(spop1o1);
		List<QuerySolution> results = new ArrayList<>();

		while(rs.hasNext()) {
			results.add(rs.next());
		}

		System.out.println(results);

		for(QuerySolution qs : results) {
			if(!qs.varNames().hasNext()) continue;
			Assert.assertTrue(qs.contains("s"));
			Assert.assertTrue(qs.contains("p"));
			Assert.assertTrue(qs.contains("o"));
			Assert.assertTrue(qs.contains("p1"));
			Assert.assertTrue(qs.contains("o1"));
		}

	}

	@Test
	public void testGroupBy() throws QueryProcException, UnsupportedEncodingException {

		ResultSet rs = runQuery(groupBy);
		List<QuerySolution> results = new ArrayList<>();

		while(rs.hasNext()){
			results.add(rs.next());
		}

		Assert.assertTrue( results.size() > 0 );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("s")) );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("nbPredicate")) );
	}

	@Test
	public void testGroupByNested() throws QueryProcException, UnsupportedEncodingException {

		ResultSet rs = runQuery(nestedGroupBy);
		List<QuerySolution> results = new ArrayList<>();

		while(rs.hasNext()){
			results.add(rs.next());
		}

		Assert.assertTrue( results.size() > 0 );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("person")) );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("numberOfSpecies")) );
	}

	@Test
	public void testGroupByAsInput() throws QueryProcException, UnsupportedEncodingException {

		ResultSet rs = runQuery(joinGroupByAsInput);
		List<QuerySolution> results = new ArrayList<>();

		while(rs.hasNext()){
			results.add(rs.next());
		}

		Assert.assertTrue( results.size() > 0 );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("person")) );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("species")) );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("animal")) );
	}

	@Test
	public void testGroupByWithInput() throws QueryProcException, UnsupportedEncodingException {

		ResultSet rs = runQuery(joinGroupByWithInput);
		List<QuerySolution> results = new ArrayList<>();

		while(rs.hasNext()){
			results.add(rs.next());
		}

		Assert.assertTrue( results.size() > 0 );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("person")) );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("species")) );
		Assert.assertTrue( results.stream().allMatch(res -> res.contains("animal")) );
	}
}
