package com.teiid.firestore.translator;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.*;
import com.teiid.firestore.connection.FirestoreConnection;
import com.teiid.firestore.translator.appenders.WhereProcessor;
import org.teiid.language.*;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    private void executeInsert(Insert insert, WriteBatch batch) {
        CollectionReference collection = connection.collection(nameInSource(insert.getTable()));
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
                        ((Array) v).getExpressions().stream().map(this::literal).collect(Collectors.toList()))
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

    private Object literal(Expression e) {
        return ((Literal) e).getValue();
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
