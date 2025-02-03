package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.federation.FederationMember;
import se.liu.ida.hefquin.engine.federation.FederationMemberAgglomeration;
import se.liu.ida.hefquin.engine.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.engine.federation.access.*;
import se.liu.ida.hefquin.engine.federation.access.impl.response.SolMapsResponseImpl;
import se.liu.ida.hefquin.engine.federation.access.utils.FederationAccessUtils;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ExecOpFrawRequest extends BaseForExecOpSolMapsRequest<DataRetrievalRequest, FederationMember>{

    List<FederationMember> endpoints;

    public ExecOpFrawRequest(DataRetrievalRequest req, FederationMemberAgglomeration fm, boolean collectExceptions) {
        super(req, fm, collectExceptions);
        this.endpoints = fm.getInterface().getFederationMembers();
    }

    @Override
    protected SolMapsResponse performRequest(FederationAccessManager fedAccessMgr) throws FederationAccessException {
        // TODO: add other cases

        Random random = new Random();
        int chosen = random.nextInt(endpoints.size());

        FederationMember chosenFM = ((FederationMemberAgglomeration) fm).getInterface().getFederationMembers().get(chosen);

        if(chosenFM instanceof SPARQLEndpoint){

            SolMapsResponse solMapsResponse = FederationAccessUtils.performRequest(fedAccessMgr, (SPARQLRequest) req, (SPARQLEndpoint) chosenFM);

            List<SolutionMapping> updatedSolutionMappingList = new ArrayList<>();

            solMapsResponse.getSolutionMappings().iterator().forEachRemaining(solutionMapping -> {
                Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, endpoints.size());
                SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                updatedSolutionMappingList.add(updatedSolutionMapping);
            });

            SolMapsResponse updatedSolMapsResponse = new SolMapsResponseImpl(updatedSolutionMappingList, fm, req, solMapsResponse.getRequestStartTime(), solMapsResponse.getRetrievalEndTime());

            return updatedSolMapsResponse;
        }

//        else if ( fm instanceof TPFServer && req instanceof TriplePatternRequest) {
//            return new ExecOpRequestTPFatTPFServer( (TriplePatternRequest) req, (TPFServer) fm, collectExceptions );
//        }
//        else if ( fm instanceof BRTPFServer && req instanceof TriplePatternRequest ) {
//            return new ExecOpRequestTPFatBRTPFServer( (TriplePatternRequest) req, (BRTPFServer) fm, collectExceptions );
//        }

        // TODO: handle this better
        throw new NotImplementedException("This operation is not implemented yet in ExecOpFrawRequest");
    }
}
