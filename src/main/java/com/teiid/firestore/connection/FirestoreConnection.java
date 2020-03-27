package com.teiid.firestore.connection;

import com.google.cloud.firestore.DocumentReference;
import org.teiid.resource.api.Connection;


public interface FirestoreConnection extends Connection {
    DocumentReference document(String documentName);
}
