package com.teiid.firestore.connection;

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.WriteBatch;
import org.teiid.resource.api.Connection;


public interface FirestoreConnection extends Connection {
    CollectionReference collection(String documentName);

    WriteBatch batch();
}
