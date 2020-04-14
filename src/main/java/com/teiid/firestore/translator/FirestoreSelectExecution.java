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
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.teiid.firestore.connection.FirestoreConnection;
import com.teiid.firestore.translator.appenders.WhereProcessor;
import com.teiid.firestore.translator.common.FirestoreCommand;
import org.teiid.language.ColumnReference;
import org.teiid.language.NamedTable;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.teiid.firestore.translator.common.TranslatorUtils.*;


/**
 * Represents the execution of a command.
 */
public class FirestoreSelectExecution implements ResultSetExecution {
    private FirestoreExecution firestoreExecution;
    private Iterator<QueryDocumentSnapshot> results;
    private String[] fields;

    FirestoreSelectExecution(Select command, FirestoreConnection firestoreConnection, WhereProcessor whereProcessor) {
        this.fields = fields(command);
        FirestoreCommand firestoreCommand = new FirestoreCommand(
                (NamedTable) command.getFrom().get(0),
                command.getWhere(),
                command.getLimit(),
                command.getOrderBy(),
                fields);
        this.firestoreExecution = new FirestoreExecution(firestoreConnection, whereProcessor, firestoreCommand);
    }

    @Override
    public void execute() throws TranslatorException {
        try {
            results = firestoreExecution.execute().iterator();
        } catch (ExecutionException | InterruptedException e) {
            throw new TranslatorException(e);
        }
    }

    private String[] fields(Select command) {
        return command.getDerivedColumns().stream()
                .map(derivedColumn -> nameInSource((ColumnReference) derivedColumn.getExpression()))
                .toArray(String[]::new);
    }

    @Override
    public List<?> next() throws DataNotAvailableException {
        if (results != null && results.hasNext()) {
            QueryDocumentSnapshot next = results.next();
            return Stream.of(fields)
                    .map(field -> {
                        if (field.equals(FieldPath.documentId().toString())) {
                            return next.getId();
                        } else if (field.endsWith(PARENT_ID_SUFFIX)) {
                            return parentId(next);
                        } else {
                            Object o = next.get(field);
                            return o instanceof List ? ((List) o).toArray() : o;
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
}
