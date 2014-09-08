package net.unit8.teslogger.comparator;

import org.h2.table.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kawasima
 */
public class Row extends ArrayList<Object> implements Cloneable {
    private List<Integer> pkIndex = new ArrayList<>();

    public Row(List<Column> columns) {
        super(columns.size());
        int i=0;
        for (Column column : columns) {
            if (column.isPrimaryKey()) {
                pkIndex.add(i);
            }
            i++;
        }
    }

    public Row diff(Row another) {
        Row newRow = (Row) this.clone();
        newRow.clear();
        for (int i=0; i < this.size(); i++) {
            Object v1 = this.get(i);
            Object v2 = another.get(i);

            if (v1 == null && v2 == null) {
                newRow.add(null);
            } else if (v1 == null || v2 == null) {
                newRow.add(new Object[]{v1, v2});
            } else if (v1.equals(v2)) {
                newRow.add(v1);
            } else {
                newRow.add(new Object[]{v1, v2});
            }
        }
        return newRow;
    }

    public boolean same(Row another) {
        if (pkIndex.size() == 0) return false;

        boolean isSame = true;
        for (Integer idx : pkIndex) {
            Object v1 = this.get(idx);
            Object v2 = another.get(idx);
            if (v1 == null || v2 == null) {
                isSame = false;
                break;
            }
            isSame &= v1.equals(v2);
        }
        return isSame;
    }
}
