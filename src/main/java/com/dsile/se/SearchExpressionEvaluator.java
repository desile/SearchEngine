package com.dsile.se;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.dsile.se.dto.IndexDocumentRecord;
import com.dsile.se.utils.IndexSearcher;
import com.fathzer.soft.javaluator.*;
import org.apache.lucene.morphology.WrongCharaterException;
import org.apache.lucene.morphology.russian.RussianLuceneMorphology;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SearchExpressionEvaluator extends AbstractEvaluator<Map<Integer, IndexDocumentRecord>> {

    private static final Pattern quoteRegex = Pattern.compile("\"(.*)\"(\\/(\\d+))?");

    /** The negate unary operator.*/
    public final static Operator NEGATE = new Operator("!", 1, Operator.Associativity.RIGHT, 3);
    /** The logical AND operator.*/
    private static final Operator AND = new Operator("&&", 2, Operator.Associativity.LEFT, 2);
    /** The logical OR operator.*/
    public final static Operator OR = new Operator("||", 2, Operator.Associativity.LEFT, 1);

    @Autowired
    private IndexSearcher indexSearcher;
    private RussianLuceneMorphology rusmorph;

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

        try {
            rusmorph = new RussianLuceneMorphology();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Map<Integer, IndexDocumentRecord> toValue(String literal, Object evaluationContext) {
        Matcher quoteMatch = quoteRegex.matcher(literal);
        if(quoteMatch.matches()){

            String[] quote = quoteMatch.group(1).split(" ");
            List normalQuote = Arrays.stream(quote).map(q -> {
                try {
                    return rusmorph.getNormalForms(q.toLowerCase()).get(0);
                } catch (WrongCharaterException wce){
                    return q.toLowerCase();
                }
            }).collect(Collectors.toList());

            int connectivity = normalQuote.size();
            if(quoteMatch.groupCount() == 3 && quoteMatch.group(3) != null){
                connectivity = Integer.parseInt(quoteMatch.group(3)) + 1;
            }

            return indexSearcher.findDocsWithQuote(normalQuote,connectivity);
        } else {
            List<String> normalForms;
            try {
                normalForms = rusmorph.getNormalForms(literal.toLowerCase());
            } catch (WrongCharaterException wce){
                normalForms = Collections.singletonList(literal);
            }

            return indexSearcher.findDocsWithWord(normalForms);
        }
    }

    public Map<Integer, IndexDocumentRecord> parseAndEvaluate(String expression, Object evaluationContext){
        StringBuilder formattedQuery = new StringBuilder();
        expression = expression.trim();
        boolean space = false;
        boolean inQuotes = false;
        for(Character c :expression.toCharArray()){
            if(c.equals('\"')){
                if(inQuotes){
                    inQuotes = false;
                } else {
                    inQuotes = true;
                }
                formattedQuery.append(c);
                continue;
            }

            if(c.equals(' ')){
                if(inQuotes){
                    formattedQuery.append(c);
                    continue;
                } else {
                    if (!space){
                        space = true;
                        formattedQuery.append("&&");
                    }
                    continue;
                }
            } else {
                space = false;
            }

            formattedQuery.append(c);
        }

        return evaluate(formattedQuery.toString(), evaluationContext);
    }

    @Override
    protected Map<Integer, IndexDocumentRecord> evaluate(Operator operator, Iterator<Map<Integer, IndexDocumentRecord>> operands, Object evaluationContext) {
        if (operator == NEGATE) {
            return antiIntersection(operands.next());
        } else if (operator == OR) {
            Map<Integer, IndexDocumentRecord> o1 = operands.next();
            Map<Integer, IndexDocumentRecord> o2 = operands.next();
            return union(o1,o2);
        } else if (operator == AND) {
            Map<Integer, IndexDocumentRecord> o1 = operands.next();
            Map<Integer, IndexDocumentRecord> o2 = operands.next();
            return intersection(o1,o2);
        } else {
            return super.evaluate(operator, operands, evaluationContext);
        }
    }

    private Map<Integer, IndexDocumentRecord> intersection(Map<Integer, IndexDocumentRecord> s1, Map<Integer, IndexDocumentRecord> s2){
        Map<Integer, IndexDocumentRecord> union = new HashMap<>();
        for(Map.Entry<Integer, IndexDocumentRecord> e : s1.entrySet()){
            if(s2.containsKey(e.getKey())){
                e.getValue().sumTfIdf(s2.get(e.getKey()).getTfIdf());
                union.put(e.getKey(),e.getValue());
            }
        }
        return union;
    }

    private Map<Integer, IndexDocumentRecord> antiIntersection(Map<Integer, IndexDocumentRecord> s2){
        return new HashMap<>(indexSearcher.allDocs().stream().filter(l -> !s2.containsKey(l)).collect(Collectors.toMap(k -> k, v -> new IndexDocumentRecord(v,0f,Collections.emptyList()))));
    }

    private Map<Integer, IndexDocumentRecord> union(Map<Integer, IndexDocumentRecord> s1, Map<Integer, IndexDocumentRecord> s2){
        for(Map.Entry<Integer, IndexDocumentRecord> e : s1.entrySet()){
            if(s2.containsKey(e.getKey())){
                e.getValue().sumTfIdf(s2.get(e.getKey()).getTfIdf());
            }
        }
        return s1;
    }

}
