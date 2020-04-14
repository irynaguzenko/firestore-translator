package com.teiid.firestore.translator.common;

import com.google.cloud.firestore.FieldPath;
import org.teiid.language.Condition;
import org.teiid.language.Limit;
import org.teiid.language.NamedTable;
import org.teiid.language.OrderBy;

import java.util.Arrays;

import static com.teiid.firestore.translator.common.TranslatorUtils.PARENT_ID_SUFFIX;

public class FirestoreCommand {
    private NamedTable namedTable;
    private Condition where;
    private Limit limit;
    private OrderBy orderBy;
    private String[] fields;
    private String[] filteredFields;

    public FirestoreCommand(NamedTable namedTable, Condition where) {
        this.namedTable = namedTable;
        this.where = where;
        this.fields = new String[]{FieldPath.documentId().toString()};
        this.filteredFields = fields;
    }

    public FirestoreCommand(NamedTable namedTable, Condition where, Limit limit, OrderBy orderBy, String[] fields) {
        this.namedTable = namedTable;
        this.where = where;
        this.limit = limit;
        this.orderBy = orderBy;
        this.fields = fields;
        this.filteredFields = Arrays.stream(fields)
                .filter(field -> !field.endsWith(PARENT_ID_SUFFIX))
                .toArray(String[]::new);
    }

    public NamedTable getNamedTable() {
        return namedTable;
    }

    public Condition getWhere() {
        return where;
    }

    public Limit getLimit() {
        return limit;
    }

    public OrderBy getOrderBy() {
        return orderBy;
    }

    public String[] getFields() {
        return fields;
    }

    public String[] getFilteredFields() {
        return filteredFields;
    }
}
