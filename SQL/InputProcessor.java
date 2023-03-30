import javax.print.DocFlavor;
import java.util.*;

public class InputProcessor {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        while (true) {
            String input = sc.nextLine();
            handle(input);
        }
    }

    private static void handle(String input) {
        if (input.toUpperCase().startsWith("CREATE TABLE")) {
            String[] tokens = input.trim().replace(";", "").split(" ", 4);

            String tableName = tokens[2];
            String[] cNames = tokens[3].replace("(", "").replace(")", "").split(",");
            ArrayList<String> columnNames = new ArrayList<>(Arrays.asList(cNames));
            Manager.createTable(tableName, columnNames);
        } else if (input.matches("INSERT INTO .*\\(.*\\) VALUES.*;")) {
            String[] tokens = input.trim().replace(";", "").split(" ");
            String tableName = tokens[2];
            String[] cNames = tokens[3].replace("(", "").replace(")", "").split(",");
            String[] cValues = tokens[5].replace("'", "").replace("(", "").replace(")", "").split(",");
            Manager.insertInto(tableName, cNames, cValues);

        } else if (input.toUpperCase().startsWith("INSERT INTO")) {
            String[] tokens = input.trim().replace(";", "").split(" ", 5);
            String tableName = tokens[2];
            String[] cNames = tokens[4].replace("(", "").replace(")", "").split(",");
            ArrayList<String> fields = new ArrayList<>();
            for (String s : cNames) {
                fields.add(s.replace("'", ""));
            }
            Manager.insertInto(tableName, fields);
        } else if (input.toUpperCase().startsWith("SELECT * FROM") && !input.contains("WHERE")) {
            String tableName = input.replace("SELECT * FROM ", "").replaceAll("( WHERE.*)?;", "");
            Manager.selectAll(tableName);
        } else if (input.toUpperCase().startsWith("SELECT * FROM") && input.contains("WHERE")) {
            String tableName = input.replaceAll("SELECT.*FROM\\s", "").replace(";", "").replaceAll(" WHERE.*", "");
            //String columnName = input.replaceAll(" FROM.*", "").replace("SELECT ", "");
            //String[] columns = columnName.split(",");
            String conditions = input.replaceAll("SELECT.*WHERE ", "").replace("'", "").replace(";", "");
            String conditionColumn = conditions.replaceAll(" =.*", "");
            String conditionValue = conditions.replaceAll(".*= ", "");
            Manager.selectCondition(tableName, conditionColumn, conditionValue);

        } else if (input.toUpperCase().startsWith("SELECT")) {
            String tableName = input.replaceAll("SELECT.*FROM\\s", "").replace(";", "").replaceAll(" WHERE.*", "");
            String columnName = input.replaceAll(" FROM.*", "").replace("SELECT ", "");
            String[] columns = columnName.split(",");
            if (input.contains("WHERE")) {
                String conditions = input.replaceAll("SELECT.*WHERE ", "").replace("';", "");
                String conditionColumn = conditions.replaceAll(" =.*", "");
                String conditionValue = conditions.replaceAll(".*= '", "");
                Manager.select(tableName, columns, conditionColumn, conditionValue);
            } else {
                Manager.select(tableName, columns);
            }
        } else if (input.toUpperCase().startsWith("DROP TABLE")) {
            String tableName = input.replace("DROP TABLE ", "").replace(";", "");
            Manager.dropTable(tableName);
        } else if (input.startsWith("show tables;")) {
            Manager.showTables();
        } else if (input.startsWith("DELETE")) {
            String tableName = input.replace("DELETE FROM ", "").replaceAll("( WHERE.*)?;", "");
            if (input.contains("WHERE")) {
                String conditions = input.replaceAll(".*WHERE ", "").replace("';", "");
                String conditionColumn = conditions.replaceAll(" =.*", "");
                String conditionValue = conditions.replaceAll(".*= '", "");
                Manager.delete(tableName, conditionColumn, conditionValue);
            } else {
                Manager.delete(tableName, null, null);
            }
        } else if (input.startsWith("UPDATE")) {
            String tableName = input.replaceAll("UPDATE ", "").replaceAll(" SET.*", "");
            String sets = input.replaceAll(".*SET ", "").replaceAll(" WHERE.*", "").replace("';", "");
            sets=sets.replaceAll("'", "");
            HashMap<String, String> map = new HashMap<>();
            for (String s : sets.split(",")) {
                String[] array = s.split(" = ");
                String key = array[0];
                String value = array[1];
                map.put(key, value);
            }
            if (input.contains("WHERE")) {
                String conditions = input.replaceAll(".*WHERE ", "").replace("';", "");
                String conditionColumn = conditions.replaceAll(" =.*", "");
                String conditionValue = conditions.replaceAll(".*= '", "");
                Manager.update(tableName, map, conditionColumn, conditionValue);
            } else {
                Manager.update(tableName, map, null, null);

            }
        }
        else if (input.equals("end"))
            System.exit(0);

    }
}

class Manager {
    private static DataBase dataBase = new DataBase();

    public static void createTable(String tableName, ArrayList<String> columnNames) {
        dataBase.createTable(tableName, columnNames);
    }

    public static void insertInto(String tableName, ArrayList<String> fields) {
        Table table = dataBase.getTable(tableName);
        if (table == null) {
            System.out.println("ERROR!");
            return;
        }
        if(fields.size()!=table.getColumnNames().size()){
            System.out.println("ERROR!");
            return;
        }
        table.insertInto(fields);
    }

    public static void selectAll(String tableName) {
        Table table = dataBase.getTable(tableName);
        if (table == null) {
            System.out.println("ERROR!");
            return;
        }
        table.selectAll();
    }

    public static void select(String tableName, String[] columns) {
        Table table = dataBase.getTable(tableName);
        if (table == null) {
            System.out.println("ERROR!");
            return;
        }
        LinkedHashSet<String> lhSetColors = new LinkedHashSet<String>(Arrays.asList(columns));
        String[] newColumns = lhSetColors.toArray(new String[ lhSetColors.size() ]);
        for (int i = 0; i < newColumns.length; i++) {
            if (!table.getColumnNames().contains(newColumns[i])) {
                System.out.println("ERROR!");
                return;
            }
        }
        table.select(newColumns, null, null);
    }

    public static void select(String tableName, String[] columns, String conditionColumn, String conditionValue) {
        LinkedHashSet<String> lhSetColors = new LinkedHashSet<String>(Arrays.asList(columns));
        String[] newColumns = lhSetColors.toArray(new String[ lhSetColors.size() ]);
        Table table = dataBase.getTable(tableName);

        if (table == null) {
            System.out.println("ERROR!");
            return;
        }

        if(!table.getColumnNames().contains(conditionColumn)){
            System.out.println("ERROR!");
            return;
        }
        for (int i = 0; i < newColumns.length; i++) {
            if (!table.getColumnNames().contains(newColumns[i])) {
                System.out.println("ERROR!");
                return;
            }
        }
        int[] indexes = new int[newColumns.length];
        for (int i = 0; i < newColumns.length; i++) {
            for(int j=0;j<table.getColumnNames().size();j++){
                if(table.getColumnNames().get(j).equals(newColumns[i])){
                    indexes[i]=j;
                }
            }
        }
//        for(int i=0;i<indexes.length;i++){
//            for(int j=i+1;j<indexes.length;j++){
//                if(indexes[i]>indexes[j]){
//                    System.out.println("ERROR!");
//                    return;
//                }
//            }
//        }
        table.select(newColumns, conditionColumn, conditionValue);
    }

    public static void dropTable(String tableName) {
        dataBase.dropTable(tableName);
    }

    public static void showTables() {
        dataBase.showTables();
    }

    public static void delete(String tableName, String conditionColumn, String conditionValue) {
        Table table = dataBase.getTable(tableName);
        if (table == null) {
            System.out.println("ERROR!");
            return;
        }
        table.delete(conditionColumn, conditionValue);
    }

    public static void update(String tableName, HashMap<String, String> map, String conditionColumn, String conditionValue) {
        Table table = dataBase.getTable(tableName);
        if (table == null) {
            System.out.println("ERROR!");
            return;
        }
        int c=0;
        if(conditionColumn!=null) {
            for (int i = 0; i < table.getColumnNames().size(); i++) {
                if (table.getColumnNames().get(i).equals(conditionColumn)) {
                    c++;
                }
            }
            if (c == 0) {
                System.out.println("ERROR!");
                return;
            }
        }
        for(String key:map.keySet()){
            if(!table.getColumnNames().contains(key)){
                System.out.println("ERROR!");
                return;
            }
        }
        table.update(map, conditionColumn, conditionValue);
    }

    public static void insertInto(String tableName, String[] cNames, String[] cValues) {
        Table table = dataBase.getTable(tableName);
        if (table == null) {
            System.out.println("ERROR!");
            return;
        }
        LinkedHashSet<String> lhSetColors =
                new LinkedHashSet<String>(Arrays.asList(cNames));

        //create array from the LinkedHashSet
        String[] newCnames = lhSetColors.toArray(new String[ lhSetColors.size() ]);
        if(newCnames.length!=cValues.length) {
            System.out.println("ERROR!");
            return;
        }
        table.insertInto(newCnames, cValues);
    }

    public static void selectCondition(String tableName, String conditionColumn, String conditionValue) {
        Table table = dataBase.getTable(tableName);
        if (table == null) {
            System.out.println("ERROR!");
            return;
        }
        if( ! (table.getColumnNames().contains(conditionColumn)) ){
            System.out.println("ERROR!");
            return;
        }
        table.selectCondition(conditionColumn, conditionValue);
    }
}

class DataBase {
    private int tableNum;
    private ArrayList<Table> tables = new ArrayList<>();

    public int getTableNum() {
        return tableNum;
    }

    public void setTableNum(int tableNum) {
        this.tableNum = tableNum;
    }

    public ArrayList<Table> getTables() {
        return tables;
    }

    public void setTables(ArrayList<Table> tables) {
        tables = tables;
    }

    public void createTable(String tableName, ArrayList<String> columnNames) {
        for(Table table : tables) {
            if(table.getTableName().equals(tableName)) {
                System.out.println("ERROR!");
                return;
            }
        }
        for(int i=0;i<columnNames.size()-1;i++) {
            for(int j=1 + i;j<columnNames.size();j++) {
                if(columnNames.get(i).equals(columnNames.get(j))) {
                    System.out.println("ERROR!");
                    return;
                }
            }
        }


        Table table = new Table(tableName, columnNames);
        tables.add(table);
        System.out.println("You have made changes to the database.");
    }

    public Table getTable(String tableName) {
        for (Table table : tables) {
            if (table.getTableName().equals(tableName)) {
                return table;
            }
        }
        return null;
    }

    public void dropTable(String tableName) {
        for (Table table : tables) {
            if (table.getTableName().equals(tableName)) {
                tables.remove(table);
                System.out.println("You have made changes to the database.");
                return;
            }
        }
        System.out.println("ERROR!");

    }


    public void showTables() {
        if (tables.size() == 0) {
            System.out.println("No result.");
            return;
        }
        System.out.println("Tablename | Records");
        for (Table table : tables) {
            System.out.println(table.getTableName() + " | " + table.getRecordNum());
        }
    }

}

class Table {
    private String tableName;
    private int columnNum;
    private ArrayList<String> columnNames = new ArrayList<>();
    private int recordNum;
    private ArrayList<Record> records = new ArrayList<>();

    public Table(String tableName, ArrayList<String> columnNames) {
        this.tableName = tableName;
        this.columnNames = columnNames;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public int getColumnNum() {
        return columnNum;
    }

    public void setColumnNum(int columnNum) {
        this.columnNum = columnNum;
    }

    public ArrayList<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(ArrayList<String> columnNames) {
        columnNames = columnNames;
    }

    public int getRecordNum() {
        return records.size();
    }

    public void setRecordNum(int recordNum) {
        this.recordNum = recordNum;
    }

    public ArrayList<Record> getRecords() {
        return records;
    }

    public void setRecords(ArrayList<Record> records) {
        records = records;
    }


    public void insertInto(ArrayList<String> values) {
        recordNum++;
        Record record = new Record(values);
        records.add(record);
        System.out.println("You have made changes to the database. Rows affected: 1");
    }




    public void selectAll() {
        if (records.size() == 0) {
            System.out.println("No result.");
            return;
        }
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            if (i != columnNames.size() - 1) {
                System.out.print(columnName + " | ");
            } else {
                System.out.print(columnName);
            }
        }
        System.out.println();
        for (Record record : records) {
            System.out.println(record);
        }
    }
    public void selectCondition(String conditionColumn, String conditionValue) {

        int columnIndex = -1;
        if (conditionColumn != null) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (conditionColumn.equals(columnNames.get(i))) {
                    columnIndex = i;
                    break;
                }
            }
        }

        boolean noResult = true;
        for (Record record : records) {
            if (conditionColumn == null || record.getFields().get(columnIndex).equals(conditionValue)) {
                noResult = false;
                break;
            }
        }

        if (noResult) {
            System.out.println("No result.");
            return;
        }
        String [] columns= new String[columnNames.size()];
        for (int i = 0; i < columnNames.size(); i++) {
            columns[i] = columnNames.get(i);
        }
        ArrayList<Integer> indexes = new ArrayList<>();
        HashSet<String> added = new HashSet<>();
        printColumns(columns);
        for (String column : columns) {
            if (added.contains(column))
                continue;
            added.add(column);
            boolean flag = false;
            for (int i = 0; i < columnNames.size(); i++) {
                if (column.equals(columnNames.get(i))) {
                    indexes.add(i);
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                System.out.println("ERROR!");
                return;
            }
        }


        for (Record record : records) {
            if (conditionColumn == null || record.getFields().get(columnIndex).equals(conditionValue))
                record.printSpecificIndexes(indexes);
        }



    }

    public void select(String[] columns, String conditionColumn, String conditionValue) {
        int columnIndex = -1;
        if (conditionColumn != null) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (conditionColumn.equals(columnNames.get(i))) {
                    columnIndex = i;
                    break;
                }
            }
        }

        boolean noResult = true;
        for (Record record : records) {
            if (conditionColumn == null || record.getFields().get(columnIndex).equals(conditionValue)) {
                noResult = false;
                break;
            }
        }

        if (noResult) {
            System.out.println("No result.");
            return;
        }

        ArrayList<Integer> indexes = new ArrayList<>();
        HashSet<String> added = new HashSet<>();
        printColumns(columns);
        for (String column : columns) {
            if (added.contains(column))
                continue;
            added.add(column);
            boolean flag = false;
            for (int i = 0; i < columnNames.size(); i++) {
                if (column.equals(columnNames.get(i))) {
                    indexes.add(i);
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                System.out.println("ERROR!");
                return;
            }
        }


        for (Record record : records) {
            if (conditionColumn == null || record.getFields().get(columnIndex).equals(conditionValue))
                record.printSpecificIndexes(indexes);
        }

    }

    private void printColumns(String[] columns) {
        for (int i = 0; i < columns.length; i++) {
            if (i == columns.length - 1)
                System.out.println(columns[i]);
            else
                System.out.print(columns[i] + " | ");
        }
    }

    public void delete(String conditionColumn, String conditionValue) {
        int columnIndex = -1;

        if (conditionColumn != null) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (conditionColumn.equals(columnNames.get(i))) {
                    columnIndex = i;
                    break;
                }
            }
        }
        int c = 0;
        for (int i = 0; i < records.size(); i++) {
            Record record = records.get(i);
            if (conditionColumn == null || record.getFields().get(columnIndex).equals(conditionValue)) {
                c++;
                records.remove(i);
                i--;
            }
        }
        System.out.println("You have made changes to the database. Rows affected: " + c);


    }

    public void update(HashMap<String, String> map, String conditionColumn, String conditionValue) {
        HashMap<Integer, String> sets = new HashMap<>();

        for (int i = 0; i < columnNames.size(); i++) {
            String cName = columnNames.get(i);
            if (map.containsKey(cName)) {
                sets.put(i, map.get(cName));
            }
        }

        int columnIndex = -1;

        if (conditionColumn != null) {
            for (int i = 0; i < columnNames.size(); i++) {
                if (conditionColumn.equals(columnNames.get(i))) {
                    columnIndex = i;
                    break;
                }
            }
        }
        int c = 0;
        for (int i = 0; i < records.size(); i++) {
            Record record = records.get(i);
            if (conditionColumn == null || record.getFields().get(columnIndex).equals(conditionValue)) {
                c++;
                for (Map.Entry<Integer, String> e : sets.entrySet()) {
                    record.getFields().set(e.getKey(), e.getValue());
                }
            }
        }

        System.out.println("You have made changes to the database. Rows affected: " + c);


    }

    public void insertInto(String[] cNames, String[] cValues) {
        String[] fields = new String[columnNames.size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = "null";
        }
        for (int i = 0; i < cNames.length; i++) {
            String s = cNames[i];
            if (columnNames.contains(s)) {
                int index = columnNames.indexOf(s);
                fields[index] = cValues[i];
            } else {
                System.out.println("ERROR!");
                return;
            }
        }
        ArrayList<String> fieldsArraylist = new ArrayList<>();
        for (String s : fields) {
            fieldsArraylist.add(s);
        }
        Record record = new Record(fieldsArraylist);
        records.add(record);
        System.out.println("You have made changes to the database. Rows affected: 1");

    }


}

class Record {
    private int recordLength;
    private ArrayList<String> fields = new ArrayList<String>();

    public Record(ArrayList<String> fields) {
        this.fields = fields;
    }

    public int getRecordLength() {
        return recordLength;
    }

    public void setRecordLength(int recordLength) {
        this.recordLength = recordLength;
    }

    public ArrayList<String> getFields() {
        return fields;
    }

    public void setFields(ArrayList<String> fields) {
        fields = fields;
    }

    @Override
    public String toString() {
        String result = "";
        for (int i = 0; i < fields.size(); i++) {
            String field = fields.get(i);
            if (i == fields.size() - 1) {
                result += field;
            } else {
                result += field + " | ";
            }
        }
        return result;
    }

    public void printSpecificIndexes(ArrayList<Integer> indexes) {
        for (int i = 0; i < indexes.size(); i++) {
            Integer index = indexes.get(i);
            if (i == indexes.size() - 1)
                System.out.println(fields.get(index));
            else
                System.out.print(fields.get(index) + " | ");
        }
    }
}

