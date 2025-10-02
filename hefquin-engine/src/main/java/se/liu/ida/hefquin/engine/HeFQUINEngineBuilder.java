package se.liu.ida.hefquin.engine;

import org.apache.jena.query.ARQ;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.engine.main.QC;
import se.liu.ida.hefquin.engine.HeFQUINEngineConfigReader.Context;
import se.liu.ida.hefquin.engine.queryplan.utils.ExecutablePlanPrinter;
import se.liu.ida.hefquin.engine.queryplan.utils.LogicalPlanPrinter;
import se.liu.ida.hefquin.engine.queryplan.utils.PhysicalPlanPrinter;
import se.liu.ida.hefquin.engine.queryproc.QueryProcessor;
import se.liu.ida.hefquin.engine.queryproc.SamplingQueryProcessor;
import se.liu.ida.hefquin.federation.access.FederationAccessManager;
import se.liu.ida.hefquin.federation.catalog.FederationCatalog;
import se.liu.ida.hefquin.federation.catalog.FederationDescriptionReader;
import se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Builder class that can be used to create a fully-wired instance of
 * {@link HeFQUINEngine}.
 */
public class HeFQUINEngineBuilder
{
	private FederationCatalog fedCat = null;
	private Model engineConf = null;
	private boolean skipExecution = false;
	private boolean isExperimentRun = false;
	private ExecutorService execFed = null;
	private ExecutorService execPlan = null;
	private LogicalPlanPrinter srcasgPrinter = null;
	private LogicalPlanPrinter lplanPrinter = null;
	private PhysicalPlanPrinter pplanPrinter = null;
	private ExecutablePlanPrinter eplanPrinter = null;

	private final int DEFAULT_THREAD_POOL_SIZE = 10;
	private final String DEFAULT_CONF_DESCR_FILE = "config/DefaultConfDescr.ttl";

	/**
	 * Sets the federation catalog to be used by the engine.
	 *
	 * @param fedCat a federation catalog
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withFederationCatalog( final FederationCatalog fedCat ) {
		this.fedCat = fedCat;
		return this;
	}

	/**
	 * Sets the federation catalog to be used by the engine.
	 *
	 * @param fedCatFile a federation catalog file
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withFederationCatalog( final String fedCatFile ) {
		this.fedCat = FederationDescriptionReader.readFromFile(fedCatFile);
		return this;
	}

	/**
	 * Sets the federation catalog to be used by the engine.
	 *
	 * @param fedCatModel a federation catalog model
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withFederationCatalog( final Model fedCatModel ) {
		this.fedCat = FederationDescriptionReader.readFromModel(fedCatModel);
		return this;
	}

	/**
	 * Sets the engine configuration to be used by the engine.
	 *
	 * @param engineConf an engine configuration
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withEngineConfiguration( final Model engineConf ) {
		this.engineConf = engineConf;
		return this;
	}

	/**
	 * Sets the engine configuration to be used by the engine.
	 *
	 * @param engineConfFile an engine configuration file
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withEngineConfiguration( final String engineConfFile ) {
		this.engineConf = RDFDataMgr.loadModel(engineConfFile);
		return this;
	}

	/**
	 * Sets the logical plan printer to be used by the engine.
	 *
	 * @param printer a logical plan printer to be used when printing the logical
	 *                plans after logical plan optimization
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withLogicalPlanPrinter( final LogicalPlanPrinter printer ) {
		this.lplanPrinter = printer;
		return this;
	}

	/**
	 * Sets the physical plan printer to be used by the engine.
	 *
	 * @param printer a physical plan printer
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withPhysicalPlanPrinter( final PhysicalPlanPrinter printer ) {
		this.pplanPrinter = printer;
		return this;
	}

	/**
	 * Sets the executable plan printer to be used by the engine.
	 *
	 * @param printer a executable plan printer
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withExecutablePlanPrinter( final ExecutablePlanPrinter printer ) {
		this.eplanPrinter = printer;
		return this;
	}

	/**
	 * Sets the source assignment printer to be used by the engine.
	 *
	 * @param printer a logical plan printer to be used when printing a source
	 *                assignment that is the input to logical plan optimization
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder withSourceAssignmentPrinter( final LogicalPlanPrinter printer ) {
		this.srcasgPrinter = printer;
		return this;
	}

	/**
	 * Sets whether the engine should skip execution after query planning.
	 *
	 * @param skip whether to skip query execution
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder setSkipExecution( final boolean skip ) {
		this.skipExecution = skip;
		return this;
	}

	/**
	 * Sets whether the engine should treat the current run as part of an experiment.
	 *
	 * @param isExperimentRun whether this is an experimental run
	 * @return this builder instance for method chaining
	 */
	public HeFQUINEngineBuilder setExperimentRun(final boolean isExperimentRun) {
		this.isExperimentRun = isExperimentRun;
		return this;
	}

	/**
	 * Returns a {@link HeFQUINEngine} instance that is created using the
	 * parameters configured via this builder.
	 *
	 * The federation catalog must be provided via {@code withFederationCatalog},
	 * default values will be used for all other components unless specified explicitly.
	 *
	 * @return an initialized {@link HeFQUINEngine} instance
	 * @throws IllegalStateException if the federation catalog has not been set
	 */
	public HeFQUINEngine build() {
		if( fedCat == null ){
			throw new IllegalStateException("No federation catalog has been set");
		}
		if ( execFed == null ) {
			execFed = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
		}
		if ( execPlan == null ) {
			execPlan = Executors.newFixedThreadPool(DEFAULT_THREAD_POOL_SIZE);
		}
		if ( engineConf == null ) {
			engineConf = RDFDataMgr.loadDataset(DEFAULT_CONF_DESCR_FILE).getDefaultModel();
		}

		// create context
		final Context ctx = new HeFQUINEngineConfigReader.Context() {
			public ExecutorService getExecutorServiceForFederationAccess() { return execFed; }
			public ExecutorService getExecutorServiceForPlanTasks() { return execPlan; }
			public FederationCatalog getFederationCatalog() { return fedCat; }
			public boolean isExperimentRun() { return isExperimentRun; }
			public boolean skipExecution() { return skipExecution; }
			public LogicalPlanPrinter getSourceAssignmentPrinter() { return srcasgPrinter; }
			public LogicalPlanPrinter getLogicalPlanPrinter() { return lplanPrinter; }
			public PhysicalPlanPrinter getPhysicalPlanPrinter() { return pplanPrinter; }
			public ExecutablePlanPrinter getExecutablePlanPrinter() { return eplanPrinter; }
		};

		// init engine
		final HeFQUINEngineConfigReader confReader = new HeFQUINEngineConfigReader();
		final FederationAccessManager fedAccessMgr = confReader.readFederationAccessManager(engineConf, ctx);
		final QueryProcessor qProc = confReader.readQueryProcessor(engineConf, ctx, fedAccessMgr);

		final HeFQUINEngine engine;

		if( qProc instanceof SamplingQueryProcessor ){
			Integer budget = confReader.readBudget(engineConf);
			Integer subBudget = confReader.readSubBudget(engineConf);
			engine = new FrawEngine(fedAccessMgr, qProc, budget, subBudget);
		} else {
			engine = new HeFQUINEngine(fedAccessMgr, qProc);
		}
		// integrate the engine into the Jena/ARQ machinery
		integrateEngineIntoJena(qProc);

		Map<HeFQUINEngine, QueryProcessor> engineToQProc = ARQ.getContext().get(FrawConstants.ENGINE_TO_QPROC);

		// No need to sync here because integrateIntoJena calls are never made in parallel, it's only called once
		// sequentially for each service during server initialization.
		engineToQProc.put(engine, qProc);

		return engine;
	}

	/**
	 * This method integrates the given processor of the HeFQUIN
	 * engine into the query processing machinery of Jena ARQ.
	 */
	protected void integrateEngineIntoJena( final QueryProcessor qProc ) {
		final OpExecutorFactory factory = FrawConstants.factory;

		ARQ.init();
		ARQ.getContext().setIfUndef(FrawConstants.ENGINE_TO_QPROC, new HashMap<>());

		QC.setFactory( ARQ.getContext(), factory );
	}

}