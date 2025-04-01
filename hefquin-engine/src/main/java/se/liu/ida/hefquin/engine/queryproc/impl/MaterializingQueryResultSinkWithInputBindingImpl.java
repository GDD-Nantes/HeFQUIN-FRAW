package se.liu.ida.hefquin.engine.queryproc.impl;

import org.apache.jena.sparql.engine.binding.Binding;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;
import se.liu.ida.hefquin.engine.queryplan.executable.impl.FrawUtils;

public class MaterializingQueryResultSinkWithInputBindingImpl extends MaterializingQueryResultSinkImpl {
    private SolutionMapping inputSolutionMapping;

    public MaterializingQueryResultSinkWithInputBindingImpl(SolutionMapping solutionMapping) {
        this.inputSolutionMapping = solutionMapping;
    }

    public MaterializingQueryResultSinkWithInputBindingImpl(Binding binding) {
        this.inputSolutionMapping = new SolutionMappingImpl(binding);
    }

    @Override
    public void send( final SolutionMapping element ) {
        SolutionMapping merged;

        if(FrawUtils.compatible(element, inputSolutionMapping)) {
            merged = FrawUtils.merge(inputSolutionMapping, element);
        }else {
            System.out.println("Warning: the input solution mapping is not compatible with the input solution mapping");
            merged = element;
        }

        super.send(merged);
    }
}
