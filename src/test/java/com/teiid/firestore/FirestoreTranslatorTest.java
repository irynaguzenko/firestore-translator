package com.teiid.firestore;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.teiid.firestore.connection.FirestoreConnection;
import com.teiid.firestore.translator.FirestoreExecutionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.*;
import org.teiid.query.parser.QueryParser;
import org.teiid.translator.TranslatorException;

import java.io.StringReader;
import java.util.Map;
import java.util.Properties;

@RunWith(SpringRunner.class)
@SpringBootTest
public class FirestoreTranslatorTest {
    private FirestoreExecutionFactory firestoreExecutionFactory;



//    @Before
//    public void init() throws TranslatorException {
//        this.firestoreExecutionFactory = new FirestoreExecutionFactory();
//        this.firestoreExecutionFactory.start();
//
//
//        Select select = (Select)getCommand(sql);
//
//        SpreadsheetSQLVisitor spreadsheetVisitor = new SpreadsheetSQLVisitor(people);
//        spreadsheetVisitor.translateSQL(select);
//        assertEquals(expectedSpreadsheetQuery, spreadsheetVisitor.getTranslatedSQL());
//    }
//
//    private Command getCommand(String sql){
//        CommandBuilder builder = new CommandBuilder(metadata());
//        return builder.getCommand(sql);
//    }
//    private QueryMetadataInterface metadata() throws Exception {
//        FirestoreConnection conn = Mockito.mock(FirestoreConnection.class);
//
////        Mockito.mock(conn.getSpreadsheetInfo()).toReturn(people);
//
//        MetadataFactory factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), "");
//        GoogleMetadataProcessor processor = new GoogleMetadataProcessor();
//        processor.process(factory, conn);
//        return new TransformationMetadata(null, new CompositeMetadataStore(factory.asMetadataStore()), null, RealMetadataFactory.SFM.getSystemFunctions(), null);
//    }
//
//    @Test
//    public void shouldSelectDocumentsFromDb() throws TranslatorException {
//        this.firestoreExecutionFactory = new FirestoreExecutionFactory();
//        firestoreExecutionFactory.start();
//        this.firestoreExecutionFactory.createResultSetExecution(getCommand())

//        CollectionReference countries = firestore.collection("countries");
//        countries.document();

//        countries.document("TOywf0YvvNeMcY23k3e2").get().get().fields
//    }

    public static Database helpParse(String ddl, DatabaseStore.Mode mode) {
        final Map<String, Datatype> dataTypes = getDataTypes();
        DatabaseStore store = new DatabaseStore() {
            @Override
            public Map<String, Datatype> getRuntimeTypes() {
                return dataTypes;
            }
            @Override
            protected TransformationMetadata getTransformationMetadata() {
                Database database = getCurrentDatabase();

                CompositeMetadataStore store = new CompositeMetadataStore(database.getMetadataStore());
                store.getRoles().clear();
                return new TransformationMetadata(DatabaseUtil.convert(database), store, null,
                        null, null).getDesignTimeMetadata();
            }
        };
        store.startEditing(true);
        store.setMode(mode);
        QueryParser.getQueryParser().parseDDL(store, new StringReader(ddl));
        store.stopEditing();
        if (store.getDatabases().isEmpty()) {
            return null;
        }
        return store.getDatabases().get(0);
    }

    public static Map<String, Datatype> getDataTypes() {
        return SystemMetadata.getInstance().getRuntimeTypeMap();
    }
}
