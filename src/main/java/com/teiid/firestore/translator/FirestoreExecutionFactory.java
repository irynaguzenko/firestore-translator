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
import org.teiid.language.QueryExpression;
import org.teiid.language.Select;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.*;

import javax.resource.cci.ConnectionFactory;

@Translator(name = "firestore", description = "Firestore custom translator")
public class FirestoreExecutionFactory extends ExecutionFactory<ConnectionFactory, FirestoreConnection> {


    public FirestoreExecutionFactory() {
    }

    @Override
    public void start() throws TranslatorException {
        super.start();
        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Firestore ExecutionFactory Started");
    }

    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, FirestoreConnection connectionFactory) {
        return new FirestoreExecution((Select) command, connectionFactory);
    }

    public boolean supportsCompareCriteriaEquals() {
        return true;
    }

    public boolean supportsCompareCriteriaOrdered() {
        return true;
    }

    public boolean supportsInCriteria() {
        return true;
    }

    @Override
    public boolean supportsOrderBy() {
        return true;
    }

    @Override
    public boolean supportsRowLimit() {
        return true;
    }

    @Override
    public boolean isSourceRequired() {
        return false;
    }

    @Override
    public boolean supportsOnlyLiteralComparison() {
        return true;
    }
}
