package com.teiid.firestore.configuration;

import com.google.cloud.firestore.Firestore;
import com.teiid.firestore.connection.FirestoreConnectionFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DataSources {

    @Bean
    public FirestoreConnectionFactory firestoreConnectionFactory(Firestore firestore) {
        return new FirestoreConnectionFactory(firestore);
    }
}
