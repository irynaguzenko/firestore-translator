package com.teiid.firestore.translator.appenders;

import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.teiid.firestore.translator.common.TranslatorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.teiid.language.*;
import org.teiid.translator.TranslatorException;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static com.teiid.firestore.translator.common.TranslatorUtils.*;
import static org.teiid.language.Comparison.Operator.*;

public class WhereProcessor {
    private static final Map<Comparison.Operator, BiFunction<Query, Pair<String, Object>, Query>> queryComparisons = Map.of(
            EQ, (q, p) -> q.whereEqualTo(p.getLeft(), p.getRight()),
            LT, (q, p) -> q.whereLessThan(p.getLeft(), p.getRight()),
            LE, (q, p) -> q.whereLessThanOrEqualTo(p.getLeft(), p.getRight()),
            GT, (q, p) -> q.whereGreaterThan(p.getLeft(), p.getRight()),
            GE, (q, p) -> q.whereGreaterThanOrEqualTo(p.getLeft(), p.getRight())
    );
    private static final Map<Comparison.Operator, BiFunction<String, String, Boolean>> documentIdComparisons = Map.of(
            EQ, String::equals,
            LT, (id, v) -> id.compareTo(v) < 0,
            LE, (id, v) -> id.compareTo(v) <= 0,
            GT, (id, v) -> id.compareTo(v) > 0,
            GE, (id, v) -> id.compareTo(v) >= 0
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
            Expression leftExpression = comparison.getLeftExpression();
            if (leftExpression instanceof Function) {
                return appendFunctionComparison(query, comparison, (Function) leftExpression);
            }
            String fieldLeftExpression = fieldName(leftExpression);
            if (isParentId(fieldLeftExpression)) return query;
            Object rightExpression = rightValue(comparison);
            Comparison.Operator operator = valueOf(comparison.getOperator().name());
            return queryComparisons.get(operator).apply(query, Pair.of(fieldLeftExpression, rightExpression));
        } else if (where instanceof In) {
            In in = (In) where;
            String leftExpression = fieldName(in.getLeftExpression());
            if (isParentId(leftExpression)) return query;
            List<Object> rightExpression = rightValue(in);
            return query.whereIn(leftExpression, rightExpression);
        } else if (where instanceof Like) {
            Like like = (Like) where;
            String leftExpression = fieldName(like.getLeftExpression());
            if (isParentId(leftExpression)) return query;
            String rightExpression = rightValue(like);
            return query.whereGreaterThanOrEqualTo(leftExpression, rightExpression)
                    .whereLessThanOrEqualTo(leftExpression, rightExpression + "\uf8ff");
        }
        throw new TranslatorException("Unsupported where clause");
    }

    public void filterCollectionGroup(List<QueryDocumentSnapshot> documents, Condition where) throws TranslatorException {
        if (where instanceof AndOr) {
            AndOr andOr = (AndOr) where;
            switch (andOr.getOperator()) {
                case AND:
                    filterCollectionGroup(documents, andOr.getLeftCondition());
                    filterCollectionGroup(documents, andOr.getRightCondition());
                    return;
                case OR:
                    throw new TranslatorException("OR is not supported");
            }
        } else if (where instanceof Comparison) {
            Comparison comparison = (Comparison) where;
            Expression leftExpression = comparison.getLeftExpression();
            if (leftExpression instanceof Function || isNotParentId(leftExpression)) return;
            String rightExpression = (String) rightValue(comparison);
            Comparison.Operator operator = valueOf(comparison.getOperator().name());
            documents.removeIf(document -> !documentIdComparisons.get(operator).apply(parentId(document), rightExpression));
        } else if (where instanceof In) {
            In in = (In) where;
            if (isNotParentId(in.getLeftExpression())) return;
            List<Object> rightExpression = rightValue(in);
            documents.removeIf(document -> !rightExpression.contains(parentId(document)));
        } else if (where instanceof Like) {
            Like like = (Like) where;
            if (isNotParentId(like.getLeftExpression())) return;
            String rightExpression = rightValue(like);
            documents.removeIf(document -> {
                String parentId = parentId(document);
                return !(rightExpression.compareTo(parentId) <= 0 && parentId.compareTo(rightExpression + "\uf8ff") <= 0);
            });
        }
    }

    @Nullable
    public String getParentIdEqualityExpressionValue(Condition where) throws TranslatorException {
        if (where == null) return null;
        if (where instanceof AndOr) {
            AndOr andOr = (AndOr) where;
            switch (andOr.getOperator()) {
                case AND:
                    String left = getParentIdEqualityExpressionValue(andOr.getLeftCondition());
                    return left != null ? left : getParentIdEqualityExpressionValue(andOr.getRightCondition());
                case OR:
                    throw new TranslatorException("OR is not supported");
            }
        } else if (where instanceof Comparison) {
            Comparison comparison = (Comparison) where;
            Expression leftExpression = comparison.getLeftExpression();
            if (leftExpression instanceof Function) return null;
            String fieldName = fieldName(leftExpression);
            return isParentId(fieldName) && EQ.equals(comparison.getOperator()) ? (String) rightValue(comparison) : null;
        }
        return null;
    }

    private Query appendFunctionComparison(Query query, Comparison comparison, Function function) throws TranslatorException {
        if (!(Boolean) rightValue(comparison))
            throw new TranslatorException("Array-contains/array-contains-any cannot be negative");
        String functionName = function.getName();
        List<Expression> parameters = function.getParameters();
        String field = fieldName(parameters.get(0));
        if (functionName.equals("array_contains")) {
            return query.whereArrayContains(field, literal(parameters.get(1)));
        } else if (functionName.equals("array_contains_any")) {
            List<Object> values = ((Array) parameters.get(1)).getExpressions().stream().map(TranslatorUtils::literal).collect(Collectors.toList());
            return query.whereArrayContainsAny(field, values);
        } else {
            throw new TranslatorException("Unknown function");
        }
    }

    private Object rightValue(Comparison comparison) {
        return literal(comparison.getRightExpression());
    }

    private List<Object> rightValue(In in) {
        return in.getRightExpressions().stream().map(TranslatorUtils::literal).collect(Collectors.toList());
    }

    private String rightValue(Like like) throws TranslatorException {
        String rightExpression = (String) literal(like.getRightExpression());
        if (!rightExpression.endsWith("%"))
            throw new TranslatorException("Unsupported LIKE expression. Only prefix filtering is allowed");
        return rightExpression.substring(0, rightExpression.length() - 1);
    }

    private String fieldName(Expression expression) {
        return nameInSource((MetadataReference) expression);
    }

    private boolean isParentId(String fieldName) {
        return fieldName.endsWith(PARENT_ID_SUFFIX);
    }

    private boolean isNotParentId(Expression expression) {
        return !isParentId(fieldName(expression));
    }
}
