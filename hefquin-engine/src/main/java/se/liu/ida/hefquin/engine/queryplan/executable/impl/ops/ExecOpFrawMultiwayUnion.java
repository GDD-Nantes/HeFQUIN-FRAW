package se.liu.ida.hefquin.engine.queryplan.executable.impl.ops;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.base.data.utils.SolutionMappingUtils;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultBlock;
import se.liu.ida.hefquin.engine.queryplan.executable.IntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.CollectingIntermediateResultElementSink;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.GenericIntermediateResultBlockImpl;
import se.liu.ida.hefquin.engine.queryproc.ExecutionContext;

import java.util.Iterator;
import java.util.Random;

import static se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants.MAPPING_PROBABILITY;

public class ExecOpFrawMultiwayUnion extends ExecOpMultiwayUnion{

    private int chosenChild;

    public ExecOpFrawMultiwayUnion(int numberOfChildren, boolean collectExceptions) {
        super(numberOfChildren, collectExceptions);
        this.chosenChild = (new Random()).nextInt(numberOfChildren);
    }

    @Override
    protected void _processBlockFromXthChild( final int x,
                                              final IntermediateResultBlock input,
                                              final IntermediateResultElementSink sink,
                                              final ExecutionContext execCxt) {
        CollectingIntermediateResultElementSink tempSink = new CollectingIntermediateResultElementSink();

        if(chosenChild == x)
            super._processBlockFromXthChild(x, input, tempSink, execCxt);

        tempSink.getCollectedSolutionMappings().forEach(
                solutionMapping -> {
                    Binding binding = solutionMapping.asJenaBinding();

                    BindingBuilder bb = BindingBuilder.create();

                    for (Iterator<Var> it = binding.vars(); it.hasNext(); ) {
                        Var var = it.next();
                        if(!var.equals(MAPPING_PROBABILITY)){
                            Node node = binding.get(var);
                            bb.add(var, node);
                        }else {
                            Double newProbability = ((Double) binding.get(MAPPING_PROBABILITY).getLiteralValue()) / numberOfChildren;
                            bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteral(String.valueOf(newProbability), XSDDatatype.XSDdouble));
                        }
                    }

                    sink.send(new SolutionMappingImpl(bb.build()));
                }
        );
    }
}

