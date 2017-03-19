package com.dsile.se;

import java.util.*;
import java.util.function.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.dsile.se.utils.IndexSearcher;
import com.fathzer.soft.javaluator.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SearchExpressionEvaluator extends AbstractEvaluator<Map<Integer, Float>> {
    /** The negate unary operator.*/
    public final static Operator NEGATE = new Operator("!", 1, Operator.Associativity.RIGHT, 3);
    /** The logical AND operator.*/
    private static final Operator AND = new Operator("&&", 2, Operator.Associativity.LEFT, 2);
    /** The logical OR operator.*/
    public final static Operator OR = new Operator("||", 2, Operator.Associativity.LEFT, 1);

    @Autowired
    private IndexSearcher indexSearcher;

    private static final Parameters PARAMETERS;

    static {
        PARAMETERS = new Parameters();
        PARAMETERS.add(AND);
        PARAMETERS.add(OR);
        PARAMETERS.add(NEGATE);
        PARAMETERS.addExpressionBracket(BracketPair.PARENTHESES);
    }

    public SearchExpressionEvaluator(){
        super(PARAMETERS);
    }

    @Override
    protected Map<Integer, Float> toValue(String literal, Object evaluationContext) {
        return indexSearcher.findDocsWithWord(literal.toLowerCase());
    }

    @Override
    protected Map<Integer, Float> evaluate(Operator operator, Iterator<Map<Integer, Float>> operands, Object evaluationContext) {
        if (operator == NEGATE) {
            return antiIntersection(operands.next());
        } else if (operator == OR) {
            Map<Integer, Float> o1 = operands.next();
            Map<Integer, Float> o2 = operands.next();
            return union(o1,o2);
        } else if (operator == AND) {
            Map<Integer, Float> o1 = operands.next();
            Map<Integer, Float> o2 = operands.next();
            return intersection(o1,o2);
        } else {
            return super.evaluate(operator, operands, evaluationContext);
        }
    }

    private Map<Integer, Float> intersection(Map<Integer, Float> s1, Map<Integer, Float> s2){
        Map<Integer, Float> intersect = new HashMap<>();
        for(Map.Entry<Integer, Float> e : s1.entrySet()){
            if(s2.containsKey(e.getKey())){
                intersect.put(e.getKey(),e.getValue() + s2.get(e.getKey()));
            }
        }
        return intersect;
    }

    private Map<Integer, Float> antiIntersection(Map<Integer, Float> s2){
        return new HashMap<>(indexSearcher.allDocs().stream().filter(l -> !s2.containsKey(l)).collect(Collectors.toMap(k -> k, v -> 0f)));
    }

    private Map<Integer, Float> union(Map<Integer, Float> s1, Map<Integer, Float> s2){
        Map<Integer, Float> union = new HashMap<>(s2);
        for(Map.Entry<Integer, Float> e : s1.entrySet()){
            if(s2.containsKey(e.getKey())){
                union.put(e.getKey(),e.getValue() + s2.get(e.getKey()));
            } else {
                union.put(e.getKey(), e.getValue());
            }
        }
        return union;
    }

}
