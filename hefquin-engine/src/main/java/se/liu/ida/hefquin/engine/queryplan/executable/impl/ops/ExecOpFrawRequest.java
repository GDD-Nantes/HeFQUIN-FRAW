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

import java.util.*;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.random;

public class ExecOpFrawRequest extends BaseForExecOpSolMapsRequest<DataRetrievalRequest, FederationMember>{

    final List<FederationMember> endpoints;
    final Map<FederationMember, Queue<SolMapsResponse>> endpoint2Cache = new HashMap<>();


    public ExecOpFrawRequest(DataRetrievalRequest req, SPARQLEndpoint fm, boolean collectExceptions) {
        super(req, fm, collectExceptions);

        if(fm instanceof FederationMemberAgglomeration){
            this.endpoints = ((FederationMemberAgglomeration) fm).getInterface().getMembers();
        }
        else{
            this.endpoints = List.of(fm);
        }
    }

    @Override
    protected SolMapsResponse performRequest(FederationAccessManager fedAccessMgr) throws FederationAccessException {
        // TODO: add other cases

        int chosen;

        chosen = random.nextInt(endpoints.size());

        FederationMember chosenFM = endpoints.get(chosen);

        if(!endpoint2Cache.containsKey(chosenFM)){
            endpoint2Cache.put(chosenFM, new LinkedList<>());
        } else if(!endpoint2Cache.get(chosenFM).isEmpty()){
            return endpoint2Cache.get(chosenFM).poll();
        }

        if(chosenFM instanceof SPARQLEndpoint){

            SolMapsResponse solMapsResponse = FederationAccessUtils.performRequest(fedAccessMgr, (SPARQLRequest) req, (SPARQLEndpoint) chosenFM);

            solMapsResponse.getSolutionMappings().iterator().forEachRemaining(solutionMapping -> {
                Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, endpoints.size(), chosen);
                SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
                SolMapsResponse smr = new SolMapsResponseImpl(List.of(updatedSolutionMapping), fm, req, solMapsResponse.getRequestStartTime(), solMapsResponse.getRetrievalEndTime());
                endpoint2Cache.get(chosenFM).offer(smr);
            });

            return endpoint2Cache.get(chosenFM).poll();
        }

        // TODO: handle this better
        throw new NotImplementedException("This operation is not implemented yet in ExecOpFrawRequest");
    }
}