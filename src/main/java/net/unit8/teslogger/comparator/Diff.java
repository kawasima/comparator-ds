package net.unit8.teslogger.comparator;

import org.h2.table.Column;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kawasima
 */
public class Diff {
    private List<String> headers = new ArrayList<>();
    private List<Row> add    = new ArrayList<>();
    private List<Row> modify = new ArrayList<>();
    private List<Row> delete = new ArrayList<>();

    public Diff(List<Column> columns) {
        for (Column column : columns) {
            headers.add(column.getName());
        }
    }
    public void add(Row row) {
        add.add(row);
    }

    public void modify(Row prev, Row next) {
        add.remove(prev);
        modify.add(next.diff(prev));
    }

    public void delete(Row row) {
        delete.add(row);
    }

    public List<Row> getAdd() {
        return add;
    }

    public List<Row> getModify() {
        return modify;
    }

    public List<Row> getDelete() {
        return delete;
    }

    public List<String> getHeaders() {
        return headers;
    }
}
