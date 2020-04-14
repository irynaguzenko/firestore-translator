package com.teiid.firestore.translator;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.teiid.firestore.connection.FirestoreConnection;
import com.teiid.firestore.translator.appenders.WhereProcessor;
import com.teiid.firestore.translator.common.FirestoreCommand;
import org.teiid.language.*;
import org.teiid.metadata.Column;
import org.teiid.translator.TranslatorException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.google.cloud.firestore.Query.Direction.ASCENDING;
import static com.google.cloud.firestore.Query.Direction.DESCENDING;
import static com.teiid.firestore.translator.common.TranslatorUtils.*;

public class FirestoreExecution {
    private FirestoreConnection connection;
    private WhereProcessor whereProcessor;
    private FirestoreCommand command;

    public FirestoreExecution(FirestoreConnection connection, WhereProcessor whereProcessor, FirestoreCommand command) {
        this.connection = connection;
        this.whereProcessor = whereProcessor;
        this.command = command;
    }

    public List<QueryDocumentSnapshot> execute() throws InterruptedException, TranslatorException, ExecutionException {
        NamedTable namedTable = command.getNamedTable();
        String collectionName = nameInSource(namedTable);
        Optional<Column> parentIdColumnMetadata = parentIdColumnMetadata(namedTable);
        return parentIdColumnMetadata.isPresent() ?
                executeSubCollectionSelect(collectionName, parentCollectionName(parentIdColumnMetadata.get())) :
                executeRootCollectionSelect(collectionName);
    }

    private List<QueryDocumentSnapshot> executeRootCollectionSelect(String collectionName) throws TranslatorException, ExecutionException, InterruptedException {
        return executeCollectionSelect(connection.collection(collectionName), command.getFields());
    }

    private List<QueryDocumentSnapshot> executeSubCollectionSelect(String collectionName, String parentCollectionName) throws TranslatorException, ExecutionException, InterruptedException {
        String parentIdEqualityExpressionValue = whereProcessor.getParentIdEqualityExpressionValue(command.getWhere());
        return parentIdEqualityExpressionValue != null ?
                executeSingleSubCollectionSelect(collectionName, parentCollectionName, parentIdEqualityExpressionValue) :
                executeSubCollectionGroupSelect(collectionName);
    }

    private List<QueryDocumentSnapshot> executeSingleSubCollectionSelect(String collectionName, String parentCollectionName, String parentIdEqualityExpressionValue) throws TranslatorException, ExecutionException, InterruptedException {
        CollectionReference subCollection = connection.collection(parentCollectionName).document(parentIdEqualityExpressionValue).collection(collectionName);
        return executeCollectionSelect(subCollection, command.getFilteredFields());
    }

    private List<QueryDocumentSnapshot> executeSubCollectionGroupSelect(String collectionName) throws TranslatorException, InterruptedException, ExecutionException {
        Query query = appendQueryCriteria(connection.collectionGroup(collectionName).select(command.getFilteredFields()));
        List<QueryDocumentSnapshot> documentSnapshots = new ArrayList<>(query.get().get().getDocuments());
        Condition where = command.getWhere();
        if (where != null) {
            whereProcessor.filterCollectionGroup(documentSnapshots, where);
        }
        Limit limit = command.getLimit();
        if (limit != null) {
            documentSnapshots = documentSnapshots.stream().limit(limit.getRowLimit()).collect(Collectors.toList());
        }
        return documentSnapshots;
    }

    private List<QueryDocumentSnapshot> executeCollectionSelect(CollectionReference collectionReference, String[] fieldsToSelect) throws TranslatorException, ExecutionException, InterruptedException {
        Query query = appendQueryCriteria(collectionReference.select(fieldsToSelect));
        Limit limit = command.getLimit();
        if (limit != null) {
            query = appendLimit(query, limit);
        }
        return Objects.requireNonNull(query).get().get().getDocuments();
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
        return query;
    }

    private Query appendOrderBy(Query query, OrderBy orderBy) throws TranslatorException {
        for (SortSpecification sortSpecification : orderBy.getSortSpecifications()) {
            String field = nameInSource((MetadataReference) sortSpecification.getExpression());
            if (field.endsWith(PARENT_ID_SUFFIX)) throw new TranslatorException("Ordering by parentId isn't supported");
            String ordering = sortSpecification.getOrdering().toString();
            query = query.orderBy(field, ordering.equals("DESC") ? DESCENDING : ASCENDING);
        }
        return query;
    }

    private Query appendLimit(Query query, Limit limit) {
        return query.limit(limit.getRowLimit());
    }
}
