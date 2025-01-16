package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.federation.access.*;
import se.liu.ida.hefquin.engine.federation.access.utils.FederationAccessUtils;

import java.util.List;
import java.util.Random;

public class ExecOpFrawRequest extends BaseForExecOpSolMapsRequest<DataRetrievalRequest, FederationMember>{

    List<FederationMember> endpoints;

    public ExecOpFrawRequest(DataRetrievalRequest req, List<FederationMember> fms, boolean collectExceptions) {
        super(req, fms.get(new Random().nextInt(fms.size())), collectExceptions);
        this.endpoints = fms;
    }

    @Override
    protected SolMapsResponse performRequest(FederationAccessManager fedAccessMgr) throws FederationAccessException {
        // TODO: add other cases
        if(fm instanceof SPARQLEndpoint && req instanceof SPARQLRequest){
            return FederationAccessUtils.performRequest(fedAccessMgr, (SPARQLRequest) req, (SPARQLEndpoint) fm);
        }

//        else if ( fm instanceof TPFServer && req instanceof TriplePatternRequest) {
//            return new ExecOpRequestTPFatTPFServer( (TriplePatternRequest) req, (TPFServer) fm, collectExceptions );
//        }
//        else if ( fm instanceof BRTPFServer && req instanceof TriplePatternRequest ) {
//            return new ExecOpRequestTPFatBRTPFServer( (TriplePatternRequest) req, (BRTPFServer) fm, collectExceptions );
//        }

        // TODO: handle this better
        return null;
    }
}
