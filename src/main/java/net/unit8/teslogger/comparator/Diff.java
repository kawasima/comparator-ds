package net.unit8.teslogger.comparator;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kawasima
 */
public class Diff {
    private List<Row> add;
    private List<Row> modify;
    private List<Row> delete;

    public Diff() {
        add = new ArrayList<>();
        modify = new ArrayList<>();
        delete = new ArrayList<>();
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
}
