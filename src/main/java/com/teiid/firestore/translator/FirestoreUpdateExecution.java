package com.teiid.firestore.translator;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import com.teiid.firestore.connection.FirestoreConnection;
import com.teiid.firestore.translator.appenders.WhereProcessor;
import com.teiid.firestore.translator.common.TranslatorUtils;
import org.teiid.language.*;
import org.teiid.metadata.Column;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.teiid.firestore.translator.common.TranslatorUtils.*;

public class FirestoreUpdateExecution implements UpdateExecution {
    private BulkCommand command;
    private FirestoreConnection connection;
    private WhereProcessor whereProcessor;
    private List<Integer> updateCounts;

    FirestoreUpdateExecution(BulkCommand command, FirestoreConnection connection, WhereProcessor whereProcessor) {
        this.command = command;
        this.connection = connection;
        this.whereProcessor = whereProcessor;
        updateCounts = new ArrayList<>();
    }

    @Override
    public void execute() throws TranslatorException {
        WriteBatch batch = connection.batch();
        List<WriteResult> writeResults = getWriteResults(batch);
        if (command instanceof Update) {
            updateCounts.add(writeResults.size());
        } else {
            writeResults.forEach(result -> updateCounts.add(1));
        }
    }

    private List<WriteResult> getWriteResults(WriteBatch batch) throws TranslatorException {
        try {
            if (command instanceof Insert) {
                executeInsert((Insert) command, batch);
            } else if (command instanceof Delete) {
                executeDelete((Delete) command, batch);
            } else if (command instanceof Update) {
                executeUpdate((Update) command, batch);
            }
            ApiFuture<List<WriteResult>> commit = batch.commit();
            return commit.get();
        } catch (InterruptedException | ExecutionException e) {
            updateCounts.add(-3);
            throw new TranslatorException(e.getMessage());
        }
    }

    private void executeInsert(Insert insert, WriteBatch batch) throws TranslatorException {
        String collectionName = nameInSource(insert.getTable());
        Optional<Column> parentIdColumn = parentIdColumnMetadata(insert.getTable());
        if (parentIdColumn.isPresent()) {
            insertToSubCollection(collectionName, parentCollectionName(parentIdColumn.get()), insert, batch);
        } else {
            insertToRootCollection(connection.collection(collectionName), insert, batch);
        }
    }

    private void insertToSubCollection(String collectionName, String parentCollectionName, Insert insert, WriteBatch batch) throws TranslatorException {
        List<ColumnReference> columns = insert.getColumns();
        int indexOfParentIdField = IntStream.range(0, columns.size())
                .filter(index -> nameInSource(columns.get(index)).endsWith(PARENT_ID_SUFFIX))
                .findFirst()
                .orElseThrow(() -> new TranslatorException("ParentId field value is missing"));
        columns.remove(indexOfParentIdField);
        if (insert.getParameterValues() == null) {
            String parentId = (String) literal(((ExpressionValueSource) insert.getValueSource()).getValues().remove(indexOfParentIdField));
            CollectionReference subCollection = connection.collection(parentCollectionName).document(parentId).collection(collectionName);
            setDocument(subCollection, columns, batch, getSingleInsertParams(insert));
        } else {
            insert.getParameterValues().forEachRemaining(parameters -> {
                String parentId = (String) parameters.remove(indexOfParentIdField);
                CollectionReference subCollection = connection.collection(parentCollectionName).document(parentId).collection(collectionName);
                setDocument(subCollection, columns, batch, parameters);
            });
        }
    }

    private void insertToRootCollection(CollectionReference collection, Insert insert, WriteBatch batch) {
        List<ColumnReference> columns = insert.getColumns();
        if (insert.getParameterValues() == null) {
            setDocument(collection, columns, batch, getSingleInsertParams(insert));
        } else {
            insert.getParameterValues().forEachRemaining(parameters -> setDocument(collection, columns, batch, parameters));
        }
    }

    private void executeDelete(Delete delete, WriteBatch batch) throws ExecutionException, InterruptedException, TranslatorException {
        selectDocuments(delete.getTable(), delete.getWhere())
                .forEach(snapshot -> batch.delete(snapshot.getReference()));
    }

    private void executeUpdate(Update update, WriteBatch batch) throws TranslatorException, ExecutionException, InterruptedException {
        Map<String, Object> changes = toMap(update.getChanges());
        selectDocuments(update.getTable(), update.getWhere())
                .forEach(snapshot -> batch.update(snapshot.getReference(), changes));
    }

    private String nameInSource(MetadataReference reference) {
        return reference.getMetadataObject().getNameInSource();
    }

    private List<?> getSingleInsertParams(Insert insert) {
        return ((ExpressionValueSource) insert.getValueSource()).getValues().stream()
                .map(v -> v instanceof Literal ? literal(v) :
                        ((Array) v).getExpressions().stream().map(TranslatorUtils::literal).collect(Collectors.toList()))
                .collect(Collectors.toList());
    }

    private void setDocument(CollectionReference collection, List<ColumnReference> columns, WriteBatch batch, List<?> parameters) {
        Map<String, Object> fieldValues = getFieldValues(columns, parameters);
        Optional<String> documentId = Optional.ofNullable((String) fieldValues.remove(FieldPath.documentId().toString()));
        batch.set(documentId.map(collection::document).orElseGet(collection::document), fieldValues);
    }

    private Map<String, Object> getFieldValues(List<ColumnReference> columns, List<?> values) {
        return IntStream.range(0, columns.size())
                .mapToObj(index -> {
                    String fieldName = nameInSource(columns.get(index));
                    Object value = values.get(index);
                    return addField(fieldName, value);
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private AbstractMap.SimpleEntry<String, Object> addField(String fieldName, Object value) {
        String[] fieldParts = fieldName.split("\\.", 2);
        if (fieldParts.length > 1) {
            AbstractMap.SimpleEntry<String, Object> nestedEntry = addField(fieldParts[1], value);
            return new AbstractMap.SimpleEntry<>(fieldParts[0], Map.of(nestedEntry.getKey(), nestedEntry.getValue()));
        }
        return new AbstractMap.SimpleEntry<>(fieldName, value);
    }

    private List<QueryDocumentSnapshot> selectDocuments(NamedTable table, Condition where) throws InterruptedException, ExecutionException, TranslatorException {
        CollectionReference collection = connection.collection(nameInSource(table));
        return whereProcessor.appendWhere(collection, where).get().get().getDocuments();
    }

    private Map<String, Object> toMap(List<SetClause> changes) {
        return changes.stream()
                .map(change -> new AbstractMap.SimpleEntry<>(nameInSource(change.getSymbol()), literal(change.getValue())))
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    @Override
    public int[] getUpdateCounts() throws DataNotAvailableException {
        return updateCounts.stream().mapToInt(Integer::intValue).toArray();
    }

    @Override
    public void close() {
        updateCounts = null;
    }

    @Override
    public void cancel() {
        close();
    }
}
