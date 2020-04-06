package com.teiid.firestore.translator;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.WriteBatch;
import com.google.cloud.firestore.WriteResult;
import com.teiid.firestore.connection.FirestoreConnection;
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
    private List<Integer> updateCounts;

    FirestoreUpdateExecution(BulkCommand command, FirestoreConnection connection) {
        this.command = command;
        this.connection = connection;
        updateCounts = new ArrayList<>();
    }

    @Override
    public void execute() throws TranslatorException {
        if (command instanceof Insert) {
            Insert insert = (Insert) command;
            CollectionReference collection = connection.collection(insert.getTable().getMetadataObject().getNameInSource());
            List<ColumnReference> columns = insert.getColumns();
            WriteBatch batch = connection.batch();
            if (insert.getParameterValues() == null) {
                setDocument(collection, columns, batch, getSingleInsertParams(insert));
            } else {
                insert.getParameterValues().forEachRemaining(parameters -> setDocument(collection, columns, batch, parameters));
            }
            ApiFuture<List<WriteResult>> commit = batch.commit();
            try {
                List<WriteResult> writeResults = commit.get();
                updateCounts.add(writeResults.size());
            } catch (InterruptedException | ExecutionException e) {
                throw new TranslatorException(e.getMessage());
            }
        }
    }

    private List<?> getSingleInsertParams(Insert insert) {
        return ((ExpressionValueSource) insert.getValueSource()).getValues().stream()
                .map(v -> ((Literal) v).getValue())
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
                    String fieldName = columns.get(index).getMetadataObject().getNameInSource();
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
