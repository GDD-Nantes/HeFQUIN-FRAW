package se.liu.ida.hefquin.cli.modules;

import org.apache.jena.cmd.ArgDecl;
import org.apache.jena.cmd.CmdArgModule;
import org.apache.jena.cmd.CmdGeneral;
import org.apache.jena.cmd.ModBase;

/**
 * Command-line argument module for specifying endpoint and authentication.
 */
public class ModServer extends ModBase
{
	protected final ArgDecl argPort = new ArgDecl( ArgDecl.HasValue, "port" );
	protected final ArgDecl argPath = new ArgDecl( ArgDecl.HasValue, "path" );
	protected final ArgDecl argConfDescr = new ArgDecl( ArgDecl.HasValue, "configurationDescription", "confDescr" );
	protected final ArgDecl argFrawConfDescr = new ArgDecl( ArgDecl.HasValue, "frawConfigurationDescription", "frawConfDescr" );
	protected final ArgDecl argFedDescr = new ArgDecl( ArgDecl.HasValue, "federationDescription", "fd" );

	protected int port;
	protected String path;
	protected String fedDescr;
	protected String confDescr;
	protected String frawConfDescr;

	@Override
	public void registerWith( final CmdGeneral cmdLine ) {
		cmdLine.getUsage().startCategory( "Settings" );

		cmdLine.add( argPort, "--port", "Server port (default: 8080)" );
		cmdLine.add( argPath, "--path", "Server path (default: \"\")" );
		cmdLine.add( argConfDescr, "--confDescr",
				"File with an RDF description of the configuration (default: DefaultConfDescr.ttl)" );
		cmdLine.add( argFrawConfDescr, "--frawConfDescr",
				"File with an RDF description of the configuration for the federated random walk engine (default: config/DefaultConfDescr.ttl, aka if no configuration is given, /raw works like a classic sparql endpoint)" );
		cmdLine.add( argFedDescr, "--federationDescription",
				"File with an RDF description of the federation (default: config/DefaultFedConf.ttl)" );
	}

	@Override
	public void processArgs( final CmdArgModule cmdLine ) {
		if ( cmdLine.contains( argPort ) ) {
			port = Integer.parseInt(cmdLine.getValue( argPort ));
		} else {
			port = 8080;
		}
		if ( cmdLine.contains( argPath ) ) {
			path = cmdLine.getValue( argPath );
		} else {
			path = "";
		}
		if ( cmdLine.contains( argConfDescr ) ) {
			confDescr = cmdLine.getValue( argConfDescr );
		} else {
			confDescr = "config/DefaultConfDescr.ttl";
		}
		if ( cmdLine.contains( argFrawConfDescr ) ) {
			frawConfDescr = cmdLine.getValue( argFrawConfDescr );
		} else {
			frawConfDescr = "config/DefaultConfDescr.ttl";
		}
		if ( cmdLine.contains( argFedDescr ) ) {
			fedDescr = cmdLine.getValue( argFedDescr );
		} else {
			fedDescr = "config/DefaultFedConf.ttl";
		}
	}

	public int getPort() {
		return port;
	}

	public String getPath() {
		return path;
	}

	public String getConfDescr() {
		return confDescr;
	}

	public String getFrawConfDescr() {
		return frawConfDescr;
	}

	public String getFederationDescription() {
		return fedDescr;
	}
}
