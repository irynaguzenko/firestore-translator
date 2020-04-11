package com.teiid.firestore.translator.common;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import org.apache.commons.lang3.StringUtils;
import org.teiid.language.MetadataReference;
import org.teiid.language.NamedTable;
import org.teiid.metadata.Column;

import java.util.Objects;
import java.util.Optional;

public class TranslatorUtils {
    public static final String PARENT_ID_SUFFIX = "__parent_name__";

    public static String nameInSource(MetadataReference reference) {
        return reference.getMetadataObject().getNameInSource();
    }

    public static String parentId(QueryDocumentSnapshot document) {
        return Objects.requireNonNull(document.getReference().getParent().getParent()).getId();
    }

    public static Optional<Column> parentIdColumnMetadata(NamedTable table) {
        return table.getMetadataObject().getColumns().stream()
                .filter(c -> c.getNameInSource().endsWith(PARENT_ID_SUFFIX))
                .findFirst();
    }

    public static String parentCollectionName(Column parentIdColumn) {
        return parentIdColumn.getNameInSource().replace(PARENT_ID_SUFFIX, StringUtils.EMPTY);
    }
}
