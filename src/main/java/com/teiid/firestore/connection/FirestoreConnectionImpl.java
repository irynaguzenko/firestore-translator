package com.teiid.firestore.connection;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import org.teiid.spring.data.BaseConnection;

public class FirestoreConnectionImpl extends BaseConnection implements FirestoreConnection {
    private final Firestore firestore;

    public FirestoreConnectionImpl(Firestore firestore) {
        this.firestore = firestore;
    }

    @Override
    public CollectionReference collection(String collectionName) {
        return firestore.collection(collectionName);
    }

    @Override
    public void close() throws Exception {
        if (firestore != null) firestore.close();
    }
}
