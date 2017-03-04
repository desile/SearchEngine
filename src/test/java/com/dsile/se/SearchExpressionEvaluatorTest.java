package com.dsile.se;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;


public class SearchExpressionEvaluatorTest {
/*
    @Test
    public void evaluationTest(){
        Map<String,SortedSet<Integer>> indexMap = new HashMap<>();

        indexMap.put("all", new TreeSet<>(Arrays.asList(1,2,3,4,5,6)));
        indexMap.put("two", new TreeSet<>(Arrays.asList(2,4,6)));
        indexMap.put("three", new TreeSet<>(Arrays.asList(1,3,5)));
        indexMap.put("four", new TreeSet<>(Arrays.asList(1,2)));
        indexMap.put("five", new TreeSet<>(Arrays.asList(3,4)));
        indexMap.put("six", new TreeSet<>(Arrays.asList(5,6)));

        SearchExpressionEvaluator evaluator = new SearchExpressionEvaluator(indexMap);
        String expression = "(two&&four)||five";
        Assert.assertEquals(new TreeSet<>(Arrays.asList(2, 3, 4)),evaluator.evaluate(expression));
        expression = "five||six";
        Assert.assertEquals(new TreeSet<>(Arrays.asList(3, 4, 5, 6)),evaluator.evaluate(expression));
        expression = "!three";
        Assert.assertEquals(new TreeSet<>(Arrays.asList(2, 4, 6)),evaluator.evaluate(expression));
    }
*/
}