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

import java.util.Iterator;

import static se.liu.ida.hefquin.jenaintegration.sparql.HeFQUINConstants.MAPPING_PROBABILITY;

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

    public static SolutionMapping merge( final SolutionMapping m1, final SolutionMapping m2 ) {
        final Binding b1 = m1.asJenaBinding();
        final Binding b2 = m2.asJenaBinding();

        return new SolutionMappingImpl( merge(b1,b2) );
    }

    public static Binding merge(Binding bind1, Binding bind2) {
        // Create binding from LHS
        BindingBuilder builder = Binding.builder(bind1);
        Iterator<Var> vIter = bind2.vars();
        // Add any variables from the RHS
        for ( ; vIter.hasNext() ; ) {
            Var v = vIter.next();
            if ( !builder.contains(v) )
                builder.add(v, bind2.get(v));
            else {
                // Checking!
                Node n1 = bind1.get(v);
                Node n2 = bind2.get(v);
                if(v.equals(MAPPING_PROBABILITY)){
                    builder.add(v, NodeFactory.createLiteral(String.valueOf((Double) n1.getLiteralValue() * (Double) n2.getLiteralValue()), XSDDatatype.XSDdouble));
                }
                if ( !n1.equals(n2) )
                    Log.warn(BindingLib.class, "merge: Mismatch : " + n1 + " != " + n2);
            }
        }

        return builder.build();
    }

}
