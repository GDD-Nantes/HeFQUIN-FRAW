package se.liu.ida.hefquin.engine.queryplan.executable.impl;

import org.apache.jena.atlas.logging.Log;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingLib;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.data.utils.Budget;
import se.liu.ida.hefquin.federation.FederationMember;
import se.liu.ida.hefquin.federation.SPARQLEndpoint;
import se.liu.ida.hefquin.federation.access.*;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.MAPPING_PROBABILITY;
import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.VAR_PROVENANCE_PREFIX;

public class FrawUtils {

    public static boolean compatible(final SolutionMapping m1, final SolutionMapping m2 ) {
        final Binding b1 = m1.asJenaBinding();
        final Binding b2 = m2.asJenaBinding();

        final Iterator<Var> it = b1.vars();
        while ( it.hasNext() ) {
            final Var v = it.next();
            // TODO : make it look nicer
            if ( b2.contains(v) && ! b2.get(v).sameValueAs(b1.get(v)) && !v.equals(MAPPING_PROBABILITY))
                return false;
        }

        return true;
    }

    public static SolutionMapping merge( final SolutionMapping m1, final SolutionMapping m2, final boolean useOuterJoinSemantic ) {
        final Binding b1 = m1.asJenaBinding();
        final Binding b2 = m2.asJenaBinding();

        return new SolutionMappingImpl( merge(b1,b2,useOuterJoinSemantic) );
    }

    public static Binding merge(Binding bind1, Binding bind2,  final boolean useOuterJoinSemantic ) {
        // Create binding from LHS
        BindingBuilder builder = Binding.builder(bind1);
        Iterator<Var> vIter = bind2.vars();
        boolean computedProba = false;
        // Add any variables from the RHS
        for ( ; vIter.hasNext() ; ) {
            Var v = vIter.next();
            if ( !builder.contains(v) && !isProbaVar(v))
                builder.add(v, bind2.get(v));
            else {
                // Checking!
                Node n1 = bind1.get(v);
                Node n2 = bind2.get(v);
                if(v.equals(MAPPING_PROBABILITY)){
                    if(!computedProba){
                        Double leftProba = Double.valueOf(String.valueOf(n1.getLiteralValue()));
                        Double rightProba = Double.valueOf(String.valueOf(n2.getLiteralValue()));

                        if(rightProba == 0 && useOuterJoinSemantic) {
                            // if we're using outer join semantics AND the probability of the optional part is 0
                            // we should leave only the left probability for the mapping
                            builder.add(v,
                                    NodeFactory.createLiteralDT(String.valueOf(leftProba),
                                            XSDDatatype.XSDdouble));
                        } else {
                            // otherwise we can go on with multiplication of probability of the left branch
                            // and the right branch
                            builder.add(v,
                                    NodeFactory.createLiteralDT(String.valueOf(leftProba * rightProba),
                                    XSDDatatype.XSDdouble));
                        }

                    } else {
                        System.out.println("Tried to compute probability of joined binding twice! This shouldn't happen");
                    }
                    // Makes sure we only compute and add the probability once. A binding should only ever contain this variable once
                    // and we only iterate over binding2 to build the joined binding, but we still check just in case.
                    computedProba = true;
                }
                if ( !n1.equals(n2) && !v.equals(MAPPING_PROBABILITY))
                    Log.warn(BindingLib.class, "merge: Mismatch : " + n1 + " != " + n2);
            }
        }

        return builder.build();
    }

    public static Binding updateProbaUnion(SolutionMapping solutionMapping, int numberOfChildren, int chosen){
        Binding binding = solutionMapping.asJenaBinding();

        BindingBuilder bb = BindingBuilder.create();

        for (Iterator<Var> it = binding.vars(); it.hasNext(); ) {
            Var var = it.next();
            if(!isProbaVar(var)){
                // Processing "normal" variables, aka not probabilities or whatever
                Node node = binding.get(var);
                bb.add(var, node);
            }else if(isProbaVar(var) && !bb.contains(MAPPING_PROBABILITY)) {
                // Processing global mapping probability
                Double newProbability = Double.valueOf(String.valueOf(binding.get(MAPPING_PROBABILITY).getLiteralValue())) / Double.valueOf(numberOfChildren);
                bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteralDT(String.valueOf(newProbability), XSDDatatype.XSDdouble));
            }
            // We never copy union objects since we always create a new one right after this
        }

        return bb.build();
    }

    public static Binding updateProvenance(SolutionMapping solutionMapping, FederationMember federationMember, Set<Var> variablesFromFM){
        Binding binding = solutionMapping.asJenaBinding();

        return updateProvenance(binding, federationMember, variablesFromFM);
    }

    public static Binding updateProvenance(Binding binding, FederationMember federationMember, Set<Var> variablesFromFM){
        BindingBuilder bb = BindingBuilder.create(binding);

        for (Var var : variablesFromFM) {
            String rawProvenance = federationMember.getInterface().toString();
            String provenance = rawProvenance.replace("SPARQL endpoint at ", "");
            bb.add(Var.alloc(VAR_PROVENANCE_PREFIX + var.getVarName()), NodeFactory.createLiteralDT(provenance, XSDDatatype.XSDstring));
        }

        return bb.build();
    }

    public static boolean isProbaVar(Var v) {
        return v.equals(MAPPING_PROBABILITY);
    }

    public static boolean isProvenanceVar(Var var){
        return var.getVarName().contains(VAR_PROVENANCE_PREFIX);
    }

    public static boolean isSpecialVar(Var v){
        // Special vars do not belong to the SPARQL query being processed, but are still present in result mappings
        return isProvenanceVar(v)
                || isProbaVar(v);
    }

    public static String destringifyBindingJson(String jsonString){
        return jsonString.substring(1, jsonString.length()-1).replace("\\\"", "\"");
    }

    /**
     * Returns <code>true</code> if the first solution mapping, b1, is
     * included in the second solution mapping, b2, where we say that
     * 'b1 is included in b2' if the variables in b1 are a proper subset
     * of the variables in b2 and the two solution mappings are compatible.
     * In other words, b1 and b2 are the same for the subset of variables
     * for which they both have bindings and, additionally, b2 has bindings
     * for additional variables.
     */
    public static boolean includedIn( final Binding b1, final Binding b2 ) {
        // First check: b1 can be included in b2 only if b1 has fewer
        // variables than b2. If that is not the case, we can immediately
        // conclude that b1 is not included in b2.
        if ( b1.size() >= b2.size() ) return false;

        // Now the main check: We iterate over the variables bound in b1 and,
        // for each of these variables, we check that
        // (1) if the variable is not a special variable :
        // (a) the variable is also bound in b2 and
        // (b) both solution mappings have the same term for the variable.
        final Iterator<Var> it = b1.vars();
        while ( it.hasNext() ) {
            final Var var = it.next();
            // check (1)
            if ( isSpecialVar(var) ) continue;
            // check (a)
            if ( ! b2.contains(var) ) return false;
            // check (b)
            if ( ! b1.get(var).equals(b2.get(var)) ) return false;
        }

        return true;
    }

    public static SolMapsResponse performRequest(final SamplingFederationAccessManager fedAccessMgr,
                                                 final SPARQLRequest req,
                                                 final SPARQLEndpoint fm,
                                                 final Budget budget)
            throws FederationAccessException
    {
        return getSolMapsResponse( fedAccessMgr.issueRequest(req, fm, budget), req, fm );
    }

    protected static SolMapsResponse getSolMapsResponse( final CompletableFuture<SolMapsResponse> futureResp,
                                                         final DataRetrievalRequest req,
                                                         final FederationMember fm )
            throws FederationAccessException
    {
        try {
            return futureResp.get();
        }
        catch ( final InterruptedException e ) {
            throw new FederationAccessException("Unexpected interruption when getting the response to a data retrieval request.", e, req, fm);
        }
        catch ( final ExecutionException e ) {
            throw new FederationAccessException("Getting the response to a data retrieval request caused an exception.", e, req, fm);
        }
    }
}
