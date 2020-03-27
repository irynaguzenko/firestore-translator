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


import com.teiid.firestore.connection.FirestoreConnection;
import org.teiid.language.DerivedColumn;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/**
 * Represents the execution of a command.
 */
public class FirestoreExecution implements ResultSetExecution {
    private ExecutionContext executionContext;
    private RuntimeMetadata metadata;
    private FirestoreConnection connection;

    // Execution state
    Iterator<List<?>> results;
    int[] neededColumns;
    private Select command;

    public FirestoreExecution(Select query) {
        this.command = query;
    }

    public FirestoreExecution(Select query, ExecutionContext executionContext, RuntimeMetadata metadata, FirestoreConnection firestoreConnection) {
        this.command = query;
        this.executionContext = executionContext;
        this.metadata = metadata;
        this.connection = firestoreConnection;
    }

    @Override
    public void execute() throws TranslatorException {
        List<DerivedColumn> derivedColumns = command.getDerivedColumns();
        derivedColumns.forEach(c -> {
            c.getAlias();
        });
//        LogManager.logDetail(LogConstants.CTX_CONNECTOR, FirestorePlugin.UTIL.getString("execute_query", new Object[] { "Firestore", command })); //$NON-NLS-1$
    }


    @Override
    public List<?> next() throws TranslatorException, DataNotAvailableException {
        if (results.hasNext()) {
            return projectRow(results.next(), neededColumns);
        }
        return null;
    }

    /**
     * @param row
     * @param neededColumns
     */
    static List<Object> projectRow(List<?> row, int[] neededColumns) {
        List<Object> output = new ArrayList<Object>(neededColumns.length);

        for (int i = 0; i < neededColumns.length; i++) {
            output.add(row.get(neededColumns[i] - 1));
        }

        return output;
    }

    @Override
    public void close() {
//        LogManager.logDetail(LogConstants.CTX_CONNECTOR, FirestorePlugin.UTIL.getString("close_query")); //$NON-NLS-1$
    }

    @Override
    public void cancel() throws TranslatorException {
//        LogManager.logDetail(LogConstants.CTX_CONNECTOR, FirestorePlugin.UTIL.getString("cancel_query")); //$NON-NLS-1$
    }
}
