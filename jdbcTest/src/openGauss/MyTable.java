package openGauss;

public class MyTable {
    String createsql;
    boolean isRow;
    int insert;
    int del;
    int update;
    public MyTable(String s, boolean row, int ins, int de, int upd){
        createsql = s;
        isRow = row;
        insert = ins;
        del = de;
        update = upd;
    }
}
