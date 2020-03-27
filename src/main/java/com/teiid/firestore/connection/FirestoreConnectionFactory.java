package com.teiid.firestore.connection;

import com.google.cloud.firestore.Firestore;
import org.teiid.spring.data.BaseConnectionFactory;

public class FirestoreConnectionFactory extends BaseConnectionFactory<FirestoreConnection> {
    private final Firestore firestore;

    public FirestoreConnectionFactory(Firestore firestore) {
        super("firestore", "spring.cloud.gcp.firestore");
        this.firestore = firestore;
    }

    @Override
    public FirestoreConnection getConnection() throws Exception {
        return new FirestoreConnectionImpl(firestore);
    }
}
