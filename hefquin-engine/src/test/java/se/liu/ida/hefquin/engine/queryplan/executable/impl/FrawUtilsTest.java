package se.liu.ida.hefquin.engine.queryplan.executable.impl;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingBuilder;
import org.junit.Assert;
import org.junit.Test;
import se.liu.ida.hefquin.base.data.SolutionMapping;
import se.liu.ida.hefquin.base.data.impl.SolutionMappingImpl;

import static se.liu.ida.hefquin.jenaintegration.sparql.FrawConstants.MAPPING_PROBABILITY;

public class FrawUtilsTest {

    private Binding buildEmptyBinding(){
        BindingBuilder bb = BindingBuilder.create();
        return bb.build();
    }

    private Binding buildBindingWithProbability(Double proba){
        BindingBuilder bb = BindingBuilder.create();
        bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteral(String.valueOf(proba)));
        return bb.build();
    }

    private Binding buildBindingWithProbabilityAndValue(Double proba, String value){
        BindingBuilder bb = BindingBuilder.create();
        bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteral(String.valueOf(proba)));
        bb.add(Var.alloc("value"), NodeFactory.createLiteral(value));
        return bb.build();
    }

    private SolutionMapping buildSolutionMappingWithProbabilityAndValue(Double proba, String value){
        Binding b1 = buildBindingWithProbabilityAndValue(proba, value);
        return new SolutionMappingImpl(b1);
    }

    @Test
    public void testCompatibleWithDifferentProbabilities(){
        SolutionMapping s1 = buildSolutionMappingWithProbabilityAndValue(0.5, "value");
        SolutionMapping s2 = buildSolutionMappingWithProbabilityAndValue(0.8, "value");

        Assert.assertTrue(FrawUtils.compatible(s1, s2));
    }

    @Test
    public void testMergeWithDifferentProbabilities(){
        SolutionMapping s1 = buildSolutionMappingWithProbabilityAndValue(0.5, "value");
        SolutionMapping s2 = buildSolutionMappingWithProbabilityAndValue(0.8, "value");

        Node expected = NodeFactory.createLiteral("0.4", XSDDatatype.XSDdouble);

        Assert.assertEquals(expected, FrawUtils.merge(s1, s2).asJenaBinding().get(MAPPING_PROBABILITY));
    }

    @Test
    public void testUpdateProbaUnionCorrectlyUpdatesProbability(){
        SolutionMapping s1 = buildSolutionMappingWithProbabilityAndValue(0.5, "value");

        Assert.assertEquals(0.05, FrawUtils.updateProbaUnion(s1, 10, 1).get(MAPPING_PROBABILITY).getLiteral());
    }

    @Test
    public void testUpdateProbaUnionCorrectlyUpdatesRandomWalkHolder(){

    }
}
