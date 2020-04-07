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


import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.teiid.firestore.connection.FirestoreConnection;
import com.teiid.firestore.translator.appenders.WhereAppender;
import org.teiid.language.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.cloud.firestore.Query.Direction.ASCENDING;
import static com.google.cloud.firestore.Query.Direction.DESCENDING;
import static com.teiid.firestore.translator.common.TranslatorUtils.nameInSource;


/**
 * Represents the execution of a command.
 */
public class FirestoreSelectExecution implements ResultSetExecution {
    private Select command;
    private FirestoreConnection connection;
    private WhereAppender whereAppender;
    private Iterator<QueryDocumentSnapshot> results;
    private String[] fields;

    FirestoreSelectExecution(Select query, FirestoreConnection firestoreConnection, WhereAppender whereAppender) {
        this.command = query;
        this.connection = firestoreConnection;
        this.whereAppender = whereAppender;
        this.fields = fields();
    }

    @Override
    public void execute() throws TranslatorException {
        String collectionName = nameInSource((MetadataReference) command.getFrom().get(0));
        Query query = connection.collection(collectionName).select(fields);

        Condition where = command.getWhere();
        if (where != null) {
            query = whereAppender.appendWhere(query, where);
        }
        OrderBy orderBy = command.getOrderBy();
        if (orderBy != null) {
            query = appendOrderBy(query, orderBy);
        }
        Limit limit = command.getLimit();
        if (limit != null) {
            query = appendLimit(query, limit);
        }

        try {
            results = Objects.requireNonNull(query).get().get().getDocuments().iterator();
        } catch (InterruptedException | ExecutionException e) {
            throw new TranslatorException(e.getMessage());
        }
    }

    private Query appendLimit(Query query, Limit limit) {
        return query.limit(limit.getRowLimit());
    }

    private Query appendOrderBy(Query query, OrderBy orderBy) {
        for (SortSpecification sortSpecification : orderBy.getSortSpecifications()) {
            String ordering = sortSpecification.getOrdering().toString();
            String field = nameInSource((MetadataReference) sortSpecification.getExpression());
            query = query.orderBy(field, ordering.equals("DESC") ? DESCENDING : ASCENDING);
        }
        return query;
    }

    @Override
    public List<?> next() throws DataNotAvailableException {
        if (results != null && results.hasNext()) {
            QueryDocumentSnapshot next = results.next();
            return Stream.of(fields)
                    .map(field -> field.equals(FieldPath.documentId().toString()) ? next.getId() : next.get(field))
                    .collect(Collectors.toList());
        }
        return null;
    }

    @Override
    public void close() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Closing the connection");
        fields = null;
        results = null;
    }

    @Override
    public void cancel() {
        close();
    }

    private String[] fields() {
        return command.getDerivedColumns().stream()
                .map(derivedColumn -> nameInSource((ColumnReference) derivedColumn.getExpression()))
                .toArray(String[]::new);
    }
}
