package com.dsile.se;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dsile.se.utils.IndexSearcher;
import com.fathzer.soft.javaluator.*;

public class SearchExpressionEvaluator extends AbstractEvaluator<Set<Integer>> {
    /** The negate unary operator.*/
    public final static Operator NEGATE = new Operator("!", 1, Operator.Associativity.RIGHT, 3);
    /** The logical AND operator.*/
    private static final Operator AND = new Operator("&&", 2, Operator.Associativity.LEFT, 2);
    /** The logical OR operator.*/
    public final static Operator OR = new Operator("||", 2, Operator.Associativity.LEFT, 1);

    private IndexSearcher indexSearcher;

    private static final Parameters PARAMETERS;

    static {
        PARAMETERS = new Parameters();
        PARAMETERS.add(AND);
        PARAMETERS.add(OR);
        PARAMETERS.add(NEGATE);
        PARAMETERS.addExpressionBracket(BracketPair.PARENTHESES);
    }

    public SearchExpressionEvaluator(IndexSearcher indexSearcher) {
        super(PARAMETERS);
        this.indexSearcher = indexSearcher;
    }

    @Override
    protected Set<Integer> toValue(String literal, Object evaluationContext) {
        return indexSearcher.findDocsWithWord(literal.toLowerCase());
    }

    @Override
    protected Set<Integer> evaluate(Operator operator, Iterator<Set<Integer>> operands, Object evaluationContext) {
        if (operator == NEGATE) {
            return antiIntersection(indexSearcher.allDocs(),operands.next());
        } else if (operator == OR) {
            Set<Integer> o1 = operands.next();
            Set<Integer> o2 = operands.next();
            return union(o1,o2);
        } else if (operator == AND) {
            Set<Integer> o1 = operands.next();
            Set<Integer> o2 = operands.next();
            return intersection(o1,o2);
        } else {
            return super.evaluate(operator, operands, evaluationContext);
        }
    }

    private Set<Integer> intersection(Set<Integer> s1, Set<Integer> s2){
        return new TreeSet<>(s1.stream().filter(s2::contains).collect(Collectors.toSet()));
    }

    private Set<Integer> antiIntersection(Set<Integer> s1, Set<Integer> s2){
        return new TreeSet<>(s1.stream().filter(l -> !s2.contains(l)).collect(Collectors.toSet()));
    }

    private Set<Integer> union(Set<Integer> s1, Set<Integer> s2){
        return new TreeSet<>(Stream.concat(s1.stream(),s2.stream()).collect(Collectors.toSet()));
    }

}