package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.data.utils.SolutionMappingUtils;
import se.liu.ida.hefquin.engine.federation.access.utils.FederationAccessUtils;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;
import se.liu.ida.hefquin.engine.queryplan.info.QueryPlanningInfo;
import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.FederationMemberAgglomeration;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.*;
import se.liu.ida.hefquin.federation.access.impl.response.SolMapsResponseImpl;

import java.util.*;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.random;

/**
 * ExecOpFrawRequest is the request operator used for federated sampling.
 * This op request has two purposes:
 * - Allow for a request operator to have multiple sources to choose from randomly everytime
 *  it is called. This is a simplification of a union of request operators that all have the same query
 *  and are evaluated on different endpoints.
 *  - Compute and update the provenance and probabilities of mappings it retrieves.
 *
 *
 *  TODO : cache wip; exec op are reinstantiated all the time throughout execution (in bind joins) so cache il always
 *  TODO : thrown away. Look at what's already in HeFQUIN ?
 */
public class ExecOpFrawRequest extends BaseForExecOpSolMapsRequest<DataRetrievalRequest, FederationMember>{

    final List<FederationMember> endpoints;
    final Map<FederationMember, Queue<SolMapsResponse>> endpoint2Cache = new HashMap<>();


    public ExecOpFrawRequest(DataRetrievalRequest req, SPARQLEndpoint fm, boolean collectExceptions, QueryPlanningInfo qpInfo) {
        super(req, fm, collectExceptions, qpInfo);

        if(fm instanceof FederationMemberAgglomeration){
            this.endpoints = ((FederationMemberAgglomeration) fm).getInterface().getMembers();
        }
        else{
            this.endpoints = List.of(fm);
        }
    }

    public ExecOpFrawRequest(ExecOpRequestSPARQL execOpRequestSPARQL) {
        super (execOpRequestSPARQL.req, execOpRequestSPARQL.fm, execOpRequestSPARQL.collectExceptions, execOpRequestSPARQL.qpInfo);

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

            // We got nothing from Raw endpoints. We want raw endpoints to always return results
            // even if empty or with a probability of 0. However, it is technically possible for raw ep to reach
            // timeout before producing anything, so this check is relevant.
            if (solMapsResponse.getSize() == 0)
                return new SolMapsResponseImpl(
                        List.of(SolutionMappingUtils.createSolutionMapping()),
                        fm,
                        req, solMapsResponse.getRequestStartTime(),
                        solMapsResponse.getRetrievalEndTime()
                );

            solMapsResponse.getResponseData().forEach(solutionMapping -> {
                // Probability
                Binding updatedBinding = FrawUtils.updateProbaUnion(solutionMapping, endpoints.size(), chosen);

                // Provenance
                Set<Var> variablesFromFM = req.getExpectedVariables().getCertainVariables();
                variablesFromFM.addAll(req.getExpectedVariables().getPossibleVariables());
                updatedBinding = FrawUtils.updateProvenance(updatedBinding, chosenFM, variablesFromFM);

                // Wrapping
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