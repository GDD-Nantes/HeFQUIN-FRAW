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
        bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteralString(String.valueOf(proba)));
        return bb.build();
    }

    private Binding buildBindingWithProbabilityAndValue(Double proba, String value){
        BindingBuilder bb = BindingBuilder.create();
        bb.add(MAPPING_PROBABILITY, NodeFactory.createLiteralString(String.valueOf(proba)));
        bb.add(Var.alloc("value"), NodeFactory.createLiteralString(value));
        return bb.build();
    }

    private SolutionMapping buildSolutionMappingWithProbabilityAndValue(Double proba, String value){
        Binding b1 = buildBindingWithProbabilityAndValue(proba, value);
        return new SolutionMappingImpl(b1);
    }

    private Node createProbaNode(Double proba){
        return NodeFactory.createLiteralDT(String.valueOf(proba), XSDDatatype.XSDdouble);
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

        Node expected = createProbaNode(0.4);

        Assert.assertEquals(expected, FrawUtils.merge(s1, s2).asJenaBinding().get(MAPPING_PROBABILITY));
    }

    @Test
    public void testMergeWithDifferentProbabilitiesWhenZero(){
        SolutionMapping s1 = buildSolutionMappingWithProbabilityAndValue(0.5, "value");
        SolutionMapping s2 = buildSolutionMappingWithProbabilityAndValue(0.0, "value");

        Node expected = createProbaNode(0.0);

        Assert.assertEquals(expected, FrawUtils.merge(s1, s2).asJenaBinding().get(MAPPING_PROBABILITY));
    }

    @Test
    public void testUpdateProbaUnionCorrectlyUpdatesProbability(){
        SolutionMapping s1 = buildSolutionMappingWithProbabilityAndValue(0.5, "value");

        Node expected = createProbaNode(0.05);

        Assert.assertEquals(expected, FrawUtils.updateProbaUnion(s1, 10, 1).get(MAPPING_PROBABILITY));
    }

    @Test
    public void testUpdateProbaUnionCorrectlyUpdatesProbabilityWhenZero(){
        SolutionMapping s1 = buildSolutionMappingWithProbabilityAndValue(0.0, "value");

        Node expected = createProbaNode(0.0);

        Assert.assertEquals(expected, FrawUtils.updateProbaUnion(s1, 10, 1).get(MAPPING_PROBABILITY));
    }

    @Test
    public void testUpdateProbaUnionCorrectlyUpdatesRandomWalkHolder(){

    }
}
