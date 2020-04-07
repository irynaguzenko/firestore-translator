package com.teiid.firestore.translator.appenders;

import com.google.cloud.firestore.Query;
import org.apache.commons.lang3.tuple.Pair;
import org.teiid.language.*;
import org.teiid.translator.TranslatorException;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.teiid.firestore.translator.common.TranslatorUtils.nameInSource;
import static org.teiid.language.Comparison.Operator.*;

public class WhereAppender {
    private static final Map<Comparison.Operator, BiFunction<Query, Pair<String, Object>, Query>> comparisons = Map.of(
            EQ, (q, p) -> q.whereEqualTo(p.getLeft(), p.getRight()),
            LT, (q, p) -> q.whereLessThan(p.getLeft(), p.getRight()),
            LE, (q, p) -> q.whereLessThanOrEqualTo(p.getLeft(), p.getRight()),
            GT, (q, p) -> q.whereGreaterThan(p.getLeft(), p.getRight()),
            GE, (q, p) -> q.whereGreaterThanOrEqualTo(p.getLeft(), p.getRight())
    );

    public Query appendWhere(Query query, Condition where) throws TranslatorException {
        if (where instanceof AndOr) {
            AndOr andOr = (AndOr) where;
            switch (andOr.getOperator()) {
                case AND:
                    return appendWhere(appendWhere(query, andOr.getLeftCondition()), andOr.getRightCondition());
                case OR:
                    throw new TranslatorException("OR is not supported");
            }
        } else if (where instanceof Comparison) {
            Comparison comparison = (Comparison) where;
            String leftExpression = nameInSource((MetadataReference) comparison.getLeftExpression());
            Object rightExpression = ((Literal) comparison.getRightExpression()).getValue();
            Comparison.Operator operator = valueOf(comparison.getOperator().name());
            return comparisons.get(operator).apply(query, Pair.of(leftExpression, rightExpression));
        } else if (where instanceof In) {
            In in = (In) where;
            String leftExpression = nameInSource((MetadataReference) in.getLeftExpression());
            List<Object> rightExpression = in.getRightExpressions().stream().map(expression -> ((Literal) expression).getValue()).collect(Collectors.toList());
            return query.whereIn(leftExpression, rightExpression);
        } else if (where instanceof Like) {
            Like like = (Like) where;
            String leftExpression = nameInSource((MetadataReference) like.getLeftExpression());
            String rightExpression = (String) ((Literal) like.getRightExpression()).getValue();
            if (!rightExpression.endsWith("%"))
                throw new TranslatorException("Unsupported LIKE expression. Only prefix filtering is allowed");
            String prefix = rightExpression.substring(0, rightExpression.length() - 1);
            return query.whereGreaterThanOrEqualTo(leftExpression, prefix).whereLessThanOrEqualTo(leftExpression, prefix + "\uf8ff");
        }
        throw new TranslatorException("Unsupported where clause");
    }
}
