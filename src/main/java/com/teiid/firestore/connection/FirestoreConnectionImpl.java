package com.teiid.firestore.connection;

import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import org.teiid.spring.data.BaseConnection;

public class FirestoreConnectionImpl extends BaseConnection implements FirestoreConnection {
    private final Firestore firestore;

    public FirestoreConnectionImpl(Firestore firestore) {
        this.firestore = firestore;
    }

    @Override
    public DocumentReference document(String documentName) {
        return firestore.document(documentName);
    }

    @Override
    public void close() throws Exception {
        if (firestore != null) firestore.close();
    }
}
