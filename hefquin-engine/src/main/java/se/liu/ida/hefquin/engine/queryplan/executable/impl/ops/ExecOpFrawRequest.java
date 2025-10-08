package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.base.data.utils.SolutionMappingUtils;
import se.liu.ida.hefquin.engine.federation.access.utils.FederationAccessUtils;
import se.liu.ida.hefquin.engine.FrawUtils;
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

 */
public class ExecOpFrawRequest extends BaseForExecOpSolMapsRequest<DataRetrievalRequest, FederationMember>{

    final List<FederationMember> endpoints;
    Budget budget;

    public ExecOpFrawRequest(DataRetrievalRequest req, SPARQLEndpoint fm, boolean collectExceptions, QueryPlanningInfo qpInfo, Budget budget) {
        super(req, fm, collectExceptions, qpInfo);

        this.budget = budget;

        if(fm instanceof FederationMemberAgglomeration){
            List<FederationMember> members = ((FederationMemberAgglomeration) fm).getInterface().getMembers();
            members.stream().forEach(member -> {
                if(!(member instanceof SPARQLEndpoint)) throw new UnsupportedOperationException("Federation member" + member.toString() + "is not a SPARQL endpoint");
            });
            this.endpoints = members;
        }
        else{
            this.endpoints = List.of(fm);
        }
    }

    public ExecOpFrawRequest(DataRetrievalRequest req, SPARQLEndpoint fm, boolean collectExceptions, QueryPlanningInfo qpInfo) {
        this(req, fm, collectExceptions, qpInfo, null);
    }

    public ExecOpFrawRequest(ExecOpRequestSPARQL execOpRequestSPARQL) {
        this(execOpRequestSPARQL.req, execOpRequestSPARQL.fm, execOpRequestSPARQL.collectExceptions, execOpRequestSPARQL.qpInfo);
    }

    public ExecOpFrawRequest(ExecOpRequestSPARQL execOpRequestSPARQL, Budget budget) {
        this(execOpRequestSPARQL.req, execOpRequestSPARQL.fm, execOpRequestSPARQL.collectExceptions, execOpRequestSPARQL.qpInfo, budget);
    }

    protected SolMapsResponse _performRequest(SamplingFederationAccessManager fedAccMan, FederationMember chosenFM, Budget budget) throws FederationAccessException {
        return FrawUtils.performRequest(fedAccMan, (SPARQLRequest) req, (SPARQLEndpoint) chosenFM, budget);
    }

    protected SolMapsResponse _performRequest(FederationAccessManager fedAccMan, FederationMember chosenFM) throws FederationAccessException {
        return FederationAccessUtils.performRequest(fedAccMan, (SPARQLRequest) req, (SPARQLEndpoint) chosenFM);
    }

    @Override
    protected SolMapsResponse performRequest(FederationAccessManager fedAccessMgr) throws FederationAccessException {

        if(Objects.isNull(budget)) throw new UnsupportedOperationException("Can't sample without a budget");

        int chosen = random.nextInt(endpoints.size());
        FederationMember chosenFM = endpoints.get(chosen);

        SolMapsResponse solMapsResponse;

        if(fedAccessMgr instanceof SamplingFederationAccessManager samFedAccMan) {
            solMapsResponse = _performRequest(samFedAccMan, chosenFM, budget);
        }
        else {
            solMapsResponse = _performRequest(fedAccessMgr, chosenFM);
        }

        Iterator<SolutionMapping> smi = solMapsResponse.getResponseData().iterator();

        // We got nothing from Raw endpoints. We want raw endpoints to always return results
        // even if empty or with a probability of 0. However, it is technically possible for raw ep to reach
        // timeout before producing anything, so this check is relevant.
        if (!smi.hasNext())
            return new SolMapsResponseImpl(
                    List.of(SolutionMappingUtils.createSolutionMapping()),
                    fm,
                    req, solMapsResponse.getRequestStartTime(),
                    solMapsResponse.getRetrievalEndTime()
            );

        // Probability
        // We only ever yield one solution mapping from a performRequest call, hence the singular .next() call
        Binding updatedBinding = FrawUtils.updateProbaUnion(smi.next(), endpoints.size(), chosen);

        // Provenance
        Set<Var> variablesFromFM = req.getExpectedVariables().getCertainVariables();
        variablesFromFM.addAll(req.getExpectedVariables().getPossibleVariables());
        updatedBinding = FrawUtils.updateProvenance(updatedBinding, chosenFM, variablesFromFM);

        // Wrapping
        SolutionMapping updatedSolutionMapping = new SolutionMappingImpl(updatedBinding);
        return new SolMapsResponseImpl(
                List.of(updatedSolutionMapping),
                fm,
                req,
                solMapsResponse.getRequestStartTime(),
                solMapsResponse.getRetrievalEndTime());
    }


    // TODO : move this to the constructor
    public void setBudget(Budget budget) {
        this.budget = budget;
    }
}