package se.liu.ida.hefquin.engine;

import org.apache.commons.collections4.MultiMapUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.jena.atlas.iterator.Iter;
import org.apache.jena.atlas.iterator.IteratorDelayedInitialization;
import org.apache.jena.atlas.lib.Pair;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.core.VarExprList;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.apache.jena.sparql.engine.binding.BindingFactory;
import org.apache.jena.sparql.engine.iterator.QueryIterPlainWrapper;
import org.apache.jena.sparql.expr.ExprAggregator;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.aggregate.Accumulator;

import java.util.*;

public class QueryIterGroupFraw extends QueryIterPlainWrapper {

    private final QueryIterator embeddedIterator;

    @Override
    public void requestCancel() {
        this.embeddedIterator.cancel();
        super.requestCancel();
    }

    @Override
    protected void closeIterator() {
        this.embeddedIterator.close();
        super.closeIterator();
    }

    public QueryIterGroupFraw(QueryIterator qIter, VarExprList groupVars, List<ExprAggregator> aggregators, ExecutionContext execCxt) {
        super(calc(qIter, groupVars, aggregators, execCxt),
                execCxt);
        this.embeddedIterator = qIter;
    }

    private static Pair<Var, Accumulator> placeholder = Pair.create((Var)null, (Accumulator)null) ;

    private static Iterator<Binding> calc(final QueryIterator iter,
                                          final VarExprList groupVarExpr,
                                          final List<ExprAggregator> aggregators,
                                          final ExecutionContext execCxt) {
        return new IteratorDelayedInitialization<Binding>() {
            @Override
            protected Iterator<Binding> initializeIterator() {

                boolean hasAggregators = ( aggregators != null && ! aggregators.isEmpty() );
                boolean hasGroupBy = ! groupVarExpr.isEmpty();
                boolean noInput = ! iter.hasNext();

                // Case: No input.
                // 1/ GROUP BY - no rows.
                // 2/ No GROUP BY, e.g. COUNT=0, the results is one row always and not handled here.
                if ( noInput ) {
                    if ( hasGroupBy )
                        // GROUP
                        return Iter.nullIterator() ;
                    if ( ! hasAggregators ) {
                        // No GROUP BY, no aggregators. One result row of no columns.
                        return Iter.singleton(BindingFactory.binding());
                    }
                    // No GROUP BY, has aggregators. Insert default values.
                    BindingBuilder builder = Binding.builder();
                    for ( ExprAggregator agg : aggregators ) {
                        Node value = agg.getAggregator().getValueEmpty();
                        if ( value == null )
                            continue;
                        Var v = agg.getVar();
                        builder.add(v, value);
                    }
                    return Iter.singleton(builder.build());
                }

                // Case: there is input.
                // Phase 1 : Create keys and aggregators per key, and pump bindings through the aggregators.
                MultiValuedMap<Binding, Pair<Var, Accumulator>> accumulators = MultiMapUtils.newListValuedHashMap();
                Map<Binding, Integer> keyToGroupCardinality = new HashMap<>();
                Double total = 0.0;
                while (iter.hasNext()) {
                    Binding b = iter.nextBinding();
                    Binding key = genKey(groupVarExpr, b, execCxt);

                    if ( !hasAggregators ) {
                        // Put in a dummy to remember the input.
                        accumulators.put(key, placeholder);
                        continue;
                    }

                    // Create if does not exist.
                    if ( !accumulators.containsKey(key) ) {
                        for ( ExprAggregator agg : aggregators ) {
                            Accumulator x = agg.getAggregator().createAccumulator();
                            Var v = agg.getVar();
                            accumulators.put(key, Pair.create(v, x));
                            keyToGroupCardinality.put(key, 0);
                        }
                    }

                    // Do the per-accumulator calculation.
                    for ( Pair<Var, Accumulator> pair : accumulators.get(key) ){
                        pair.getRight().accumulate(b, execCxt);
                        keyToGroupCardinality.put(key, keyToGroupCardinality.get(key) + 1);
                        total++;
                    }


                }

                // Phase 2 : There was input and so there are some groups.
                // For each bucket, get binding, add aggregator values to the binding.
                // We used AccNull so there are always accumulators.

                if ( !hasAggregators )
                    // We used placeholder so there are always the key.
                    return accumulators.keySet().iterator();

                List<Binding> results = new ArrayList<>();
                for ( Binding k : accumulators.keySet() ) {
                    BindingBuilder builder2 = Binding.builder(k);
                    Collection<Pair<Var, Accumulator>> accs = accumulators.get(k);

                    for ( Pair<Var, Accumulator> pair : accs ) {
                        NodeValue value;
                        Double groupScalingFactor = keyToGroupCardinality.get(k) / total;

                        if(pair.getRight() instanceof RawCountAggregator.RawCountAccumulator) {
                            value = ((RawCountAggregator.RawCountAccumulator) pair.getRight()).getValueWithFactor(groupScalingFactor);
                        }else if(false){
                            // Add other raw accumulators here
                        }else {
                            value = pair.getRight().getValue();
                        }

                        if ( value == null )
                            continue;
                        Var v = pair.getLeft();
                        builder2.add(v, value.asNode());
                    }
                    results.add(builder2.build());
                }
                return results.iterator();
            }
        };
    }

    static private Binding genKey(VarExprList vars, Binding binding, ExecutionContext execCxt) {
        return copyProject(vars, binding, execCxt);
    }

    static private Binding copyProject(VarExprList vars, Binding binding, ExecutionContext execCxt) {
        // No group vars (implicit or explicit) => working on whole result set.
        // Still need a BindingMap to assign to later.
        BindingBuilder x = Binding.builder();
        for ( Var var : vars.getVars() ) {
            Node node = vars.get(var, binding, execCxt);
            // Null returned for unbound and error.
            if ( node != null ) {
                x.add(var, node);
            }
        }
        return x.build();
    }
}
