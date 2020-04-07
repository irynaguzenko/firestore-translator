package com.teiid.firestore.translator.common;

import org.teiid.language.MetadataReference;

public class TranslatorUtils {

    public static String nameInSource(MetadataReference reference) {
        return reference.getMetadataObject().getNameInSource();
    }
}
