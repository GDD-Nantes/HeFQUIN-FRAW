package se.liu.ida.hefquin.engine.queryproc.impl.srcsel;

import fr.gdd.fedqpl.visitors.ReturningArgsOpVisitorRouter;
import fr.gdd.fedqpl.visitors.ReturningOpBaseVisitor;
import fr.gdd.fedqpl.visitors.ReturningOpVisitorRouter;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.expr.ExprList;
import se.liu.ida.hefquin.engine.OpVisitorRouter;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class JOUConverter extends ReturningOpBaseVisitor {

    public Op convert(Op op) {
        if(Objects.isNull(op)) return null;
        return ReturningOpVisitorRouter.visit(this, op);
    }

    @Override
    public Op visit(OpUnion op) {
        return convertOp(op);
    }

    public static Op convertOp(Op op) {

        List<Op> subOps = getChildrenOfUnionTree(op);

        List<List<Op>> equivalentOpLists = getListsOfEquivalentOps(subOps);

        List<Op> convertedSubPlans = equivalentOpLists
                .stream()
                .map(JOUConverter::convertEquivalentPlansToJoin)
                .collect(Collectors.toList());

        if (convertedSubPlans.size() == 1) return convertedSubPlans.get(0);

        Op root = convertedSubPlans.get(0);

        for(int i = 1; i < convertedSubPlans.size(); i++){
            root = OpUnion.create(root, convertedSubPlans.get(i));
        }

        return root;
    }

    private static List<Op> getChildrenOfUnionTree(Op op){
        if( ! ( op instanceof OpUnion ) ) return List.of(op);
        OpUnion opUnion = (OpUnion) op;

        List<Op> returnList = new ArrayList<>();
        returnList.addAll(getChildrenOfUnionTree(opUnion.getLeft()));
        returnList.addAll(getChildrenOfUnionTree(opUnion.getRight()));

        return returnList;
    }

    private static List<List<Op>> getListsOfEquivalentOps(List<Op> ops) {
        EquivalentSetMap<Op> esm = new EquivalentSetMap<>(JOUConverter::areEquivalent);

        for(Op op : ops) {
            esm.put(op);
        }

        return esm.map.values().stream().toList();
    }

    // returns true if two ops are equal, except for the value of the service nodes (same triples, joins, etc...)
    public static boolean areEquivalent(Op op1, Op op2) {
        return ReturningArgsOpVisitorRouter.visit(new EquivalentOpVisitor(), op1, op2);
    }

    private static Op convertEquivalentPlansToJoin(List<Op> ops) {
        // Main method : given a list of joins of the same triples (but against different sources), create a single
        // join of unions of the same triple with different sources.

        TripleToSourceAndFilterExtractor extractor = new TripleToSourceAndFilterExtractor();


        for(Op op : ops) OpVisitorRouter.visit(extractor, op);

        if( extractor.triples2Source.isEmpty() ) throw new IllegalArgumentException("Empty triples");

        List<Op> opTriplesAgainstSources = new ArrayList<>();

        for(Triple key : extractor.triples2Source.keySet().stream().toList()) {
            opTriplesAgainstSources.add(createUnionOfSourcesForTriple(key, new ArrayList<>(extractor.triples2Source.get(key))));
        }

        Op root = opTriplesAgainstSources.get(0);

        for(int i = 1; i < opTriplesAgainstSources.size(); i++){
            root = OpJoin.create(root, opTriplesAgainstSources.get(i));
        }

        if( ! extractor.filters.isEmpty() ) root = OpFilter.filterBy(ExprList.create(extractor.filters), root);

        return root;
    }

    private static Op createUnionOfSourcesForTriple(Triple triple, List<Node> sourceNodes) {

        if( sourceNodes == null || sourceNodes.isEmpty()) throw new IllegalArgumentException("Null or empty source nodes");

        if( triple == null) throw new IllegalArgumentException("Null triple");

        OpTriple opTriple = new OpTriple(triple);

        Op root = new OpService(sourceNodes.get(0), opTriple, false);

        if(sourceNodes.size() == 1) return root;

        for(int i = 1; i < sourceNodes.size(); i++) {
            // is it okay to always use the same optriple?
            Op added = new OpService(sourceNodes.get(i), opTriple, false);
            root = OpUnion.create(root, added);
        }

        return root;
    }

    private static class EquivalentSetMap<K> {
        Map<K, List<K>> map;
        BiFunction<K, K, Boolean> equivalent;

        public EquivalentSetMap(BiFunction<K, K, Boolean> f) {
            map = new HashMap<>();
            equivalent = f;
        }

        public void put(K key) {
            boolean added = false;

            for(K k : map.keySet()) {
                if(added) break;
                if(equivalent.apply(k, key)) added = map.get(k).add(key);
            }

            if(!added) {
                List list = new ArrayList();
                list.add(key);
                map.put(key, list);
            }
        }
    }

}
