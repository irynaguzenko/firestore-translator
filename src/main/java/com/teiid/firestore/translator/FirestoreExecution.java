/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.teiid.firestore.translator;


import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.teiid.firestore.connection.FirestoreConnection;
import org.apache.commons.lang3.tuple.Pair;
import org.teiid.language.*;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.teiid.language.Comparison.Operator.*;


/**
 * Represents the execution of a command.
 */
public class FirestoreExecution implements ResultSetExecution {
    private Select command;
    private FirestoreConnection connection;
    private static final Map<Comparison.Operator, BiFunction<Query, Pair<String, Object>, Query>> comparisons = Map.of(
            EQ, (q, p) -> q.whereEqualTo(p.getLeft(), p.getRight()),
            NE, (q, p) -> q.whereGreaterThan(p.getLeft(), p.getRight()).whereLessThan(p.getLeft(), p.getRight()),
            LT, (q, p) -> q.whereLessThan(p.getLeft(), p.getRight()),
            LE, (q, p) -> q.whereLessThanOrEqualTo(p.getLeft(), p.getRight()),
            GT, (q, p) -> q.whereGreaterThan(p.getLeft(), p.getRight()),
            GE, (q, p) -> q.whereGreaterThanOrEqualTo(p.getLeft(), p.getRight())
    );
    private Iterator<QueryDocumentSnapshot> results;
    private String[] fields;

    FirestoreExecution(Select query, FirestoreConnection firestoreConnection) {
        this.command = query;
        this.connection = firestoreConnection;
        this.fields = fields();
    }

    @Override
    public void execute() throws TranslatorException {
        String collectionName = nameInSource((MetadataReference) command.getFrom().get(0));
        Query query = connection.collection(collectionName).select(fields);

        Condition where = command.getWhere();
        if (where != null) {
            query = processWhere(query, where);
        }

        try {
            results = Objects.requireNonNull(query).get().get().getDocuments().iterator();
        } catch (InterruptedException | ExecutionException e) {
            throw new TranslatorException(e.getMessage());
        }
    }

    private Query processWhere(Query query, Condition where) throws TranslatorException {
        if (where instanceof AndOr) {
            AndOr andOr = (AndOr) where;
            switch (andOr.getOperator()) {
                case AND:
                    return processWhere(processWhere(query, andOr.getLeftCondition()), andOr.getRightCondition());
                case OR:
                    throw new TranslatorException("OR is not supported");
            }
        } else if (where instanceof Comparison) {
            Comparison comparison = (Comparison) where;
            String leftExpression = nameInSource((MetadataReference) comparison.getLeftExpression());
            Object rightExpression = ((Literal) comparison.getRightExpression()).getValue();
            Comparison.Operator operator = valueOf(comparison.getOperator().name());
            return comparisons.get(operator).apply(query, Pair.of(leftExpression, rightExpression));
        }
        return null;
    }

    @Override
    public List<?> next() throws DataNotAvailableException {
        if (results != null && results.hasNext()) {
            QueryDocumentSnapshot next = results.next();
            return Stream.of(fields).map(next::get).collect(Collectors.toList());
        }
        return null;
    }

    @Override
    public void close() {
        fields = null;
        results = null;
    }

    @Override
    public void cancel() {
        fields = null;
        results = null;
    }

    private String nameInSource(MetadataReference reference) {
        return reference.getMetadataObject().getNameInSource();
    }

    private String[] fields() {
        return command.getDerivedColumns().stream().map(derivedColumn -> {
            ColumnReference column = (ColumnReference) derivedColumn.getExpression();
            return column.getMetadataObject().getNameInSource();
        }).toArray(String[]::new);
    }
}
