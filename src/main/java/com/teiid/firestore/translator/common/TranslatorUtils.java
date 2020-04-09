package com.teiid.firestore.translator.common;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import org.teiid.language.MetadataReference;

import java.util.Objects;

public class TranslatorUtils {
    public static final String PARENT_ID = "__parent_name__";

    public static String nameInSource(MetadataReference reference) {
        return reference.getMetadataObject().getNameInSource();
    }

    public static String parentId(QueryDocumentSnapshot document) {
        return Objects.requireNonNull(document.getReference().getParent().getParent()).getId();
    }
}
