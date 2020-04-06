package com.teiid.firestore.translator;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.FieldPath;
import com.google.cloud.firestore.WriteResult;
import com.teiid.firestore.connection.FirestoreConnection;
import org.teiid.language.*;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
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
//            TODO: multipleValueStatements
            Insert insert = (Insert) command;
            CollectionReference collection = connection.collection(insert.getTable().getMetadataObject().getNameInSource());
            Map<String, Object> fieldValues = getFieldValues(insert);
            Optional<String> documentId = Optional.ofNullable((String) fieldValues.remove(FieldPath.documentId().toString()));
            ApiFuture<WriteResult> future = documentId.map(collection::document).orElseGet(collection::document).set(fieldValues);
            try {
                WriteResult writeResult = future.get();
                updateCounts.add(1);
                LogManager.logDetail(LogConstants.CTX_CONNECTOR, "Inserted " + writeResult.getUpdateTime());
            } catch (InterruptedException | ExecutionException e) {
                throw new TranslatorException(e.getMessage());
            }
        }
    }

    private Map<String, Object> getFieldValues(Insert insert) {
        List<Expression> values = ((ExpressionValueSource) insert.getValueSource()).getValues();
        List<ColumnReference> columns = insert.getColumns();
        return IntStream.range(0, columns.size())
                .mapToObj(index -> {
                    String fieldName = columns.get(index).getMetadataObject().getNameInSource();
                    Object value = ((Literal) values.get(index)).getValue();
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
