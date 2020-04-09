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
import com.teiid.firestore.translator.appenders.WhereProcessor;
import org.teiid.language.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.cloud.firestore.Query.Direction.ASCENDING;
import static com.google.cloud.firestore.Query.Direction.DESCENDING;
import static com.teiid.firestore.translator.common.TranslatorUtils.*;


/**
 * Represents the execution of a command.
 */
public class FirestoreSelectExecution implements ResultSetExecution {
    private Select command;
    private FirestoreConnection connection;
    private WhereProcessor whereProcessor;
    private Iterator<QueryDocumentSnapshot> results;
    private String[] fields;

    FirestoreSelectExecution(Select query, FirestoreConnection firestoreConnection, WhereProcessor whereProcessor) {
        this.command = query;
        this.connection = firestoreConnection;
        this.whereProcessor = whereProcessor;
        this.fields = fields();
    }

    @Override
    public void execute() throws TranslatorException {
        String collectionName = nameInSource((MetadataReference) command.getFrom().get(0));
        boolean isCollectionGroup = ((NamedTable) command.getFrom().get(0)).getMetadataObject().getColumns().stream()
                .map(AbstractMetadataRecord::getNameInSource)
                .anyMatch(PARENT_ID::equals);
        try {
            results = isCollectionGroup ?
                    executeSubCollectionGroupSelect(collectionName) :
                    executeRootCollectionSelect(collectionName);
        } catch (ExecutionException | InterruptedException e) {
            throw new TranslatorException(e);
        }
    }

    private Iterator<QueryDocumentSnapshot> executeRootCollectionSelect(String collectionName) throws TranslatorException, ExecutionException, InterruptedException {
        Query query = appendQueryCriteria(connection.collection(collectionName).select(this.fields));
        return Objects.requireNonNull(query).get().get().getDocuments().iterator();
    }

    private Iterator<QueryDocumentSnapshot> executeSubCollectionGroupSelect(String collectionName) throws TranslatorException, ExecutionException, InterruptedException {
        String[] filteredFields = Arrays.stream(fields)
                .filter(field -> !PARENT_ID.equals(field))
                .toArray(String[]::new);
        Query query = appendQueryCriteria(connection.collectionGroup(collectionName).select(filteredFields));
        List<QueryDocumentSnapshot> documentSnapshots = new ArrayList<>(query.get().get().getDocuments());
        Condition where = command.getWhere();
        if (where != null) {
            whereProcessor.filterCollectionGroup(documentSnapshots, where);
        }
        return documentSnapshots.iterator();
    }

    private Query appendQueryCriteria(Query query) throws TranslatorException {
        Condition where = command.getWhere();
        if (where != null) {
            query = whereProcessor.appendWhere(query, where);
        }

        OrderBy orderBy = command.getOrderBy();
        if (orderBy != null) {
            query = appendOrderBy(query, orderBy);
        }

        Limit limit = command.getLimit();
        if (limit != null) {
            query = appendLimit(query, limit);
        }
        return query;
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
                    .map(field -> {
                        if (field.equals(FieldPath.documentId().toString())) {
                            return next.getId();
                        } else if (field.equals(PARENT_ID)) {
                            return parentId(next);
                        } else {
                            return next.get(field);
                        }
                    })
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
