
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.utd.faradaybase.FaradayBaseApplication;

import java.time.LocalTime;
import java.time.temporal.ChronoField;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FaradayTest {

    @BeforeAll
    public static void init() {
        FaradayBaseApplication.initFileSystem();
    }

    @Test
    public void testSingleDataTypeEntry() {
        Map<String, List<String>> dataMap = new LinkedHashMap<>();
        dataMap.put("tinyint", Arrays.asList("2"));
        dataMap.put("smallint", Arrays.asList("100"));
        dataMap.put("int", Arrays.asList("10033"));
        dataMap.put("bigint", Arrays.asList("8726463984"));
        dataMap.put("float", Arrays.asList("423.25"));
        dataMap.put("double", Arrays.asList("2.25432234"));
        dataMap.put("year", Arrays.asList("1988", "2022"));
        dataMap.put("time", Arrays.asList(String.valueOf(LocalTime.now().get(ChronoField.MILLI_OF_DAY))));
        dataMap.put("datetime", Arrays.asList("2027-03-04_10:10:10"));
        dataMap.put("date", Arrays.asList("2027-03-04_10:10:10"));
        dataMap.put("text", Arrays.asList("Everything is working!"));

        for (Map.Entry<String, List<String>> entry : dataMap.entrySet()) {
            String tableName = "tab_" + entry.getKey();
            String columnName = "col_" + entry.getKey();

            drop(tableName);
            create(tableName, entry.getKey());

            List<String> commands = new ArrayList<>();
            commands.addAll(entry.getValue().stream().map(s -> String.format("insert into %s (%s) values (%s)", tableName, columnName, s)).collect(Collectors.toList()));
            commands.forEach(c -> execute(c));

            select(tableName);

        }
    }

    @Test
    public void testMultipleDataTypeEntry() {
        Map<String, String> dataMap = getDefaultDataMap();
        String tableName = "table_multiple";
        drop(tableName);
        create(tableName, dataMap.keySet());
        insert(tableName, dataMap);
        select(tableName);
    }

    @Test
    public void testUpdateTable() {
        String tableName = "table_update";
        Map<String, String> dataMap = getDefaultDataMap();
        drop(tableName);
        create(tableName, dataMap.keySet());

        dataMap.put("tinyint", "2");
        insert(tableName, dataMap);
        insert(tableName, dataMap);

        dataMap.put("tinyint", "5");
        insert(tableName, dataMap);

        select(tableName);

        String updateQuery = String.format("update %s set col_smallint = 200 where col_tinyint = 2", tableName);
        execute(updateQuery);

        select(tableName);
    }

    @Test
    public void createTableWithUniqueColumn() {
        drop("davisbase_teams");
        execute("create table davisbase_teams(team_id int unique, team_name text)");
        execute("insert into davisbase_teams(team_id, team_name) values(1, faraday)");
        execute("insert into davisbase_teams(team_id, team_name) values(1, faraday2)");
        select("davisbase_teams");
    }

    @Test
    public void createTableWithNotNullColumn() {
        drop("davisbase_teams");
        execute("create table davisbase_teams(team_id int, team_name text not null)");
        execute("insert into davisbase_teams(team_id, team_name) values(1, faraday)");
        execute("insert into davisbase_teams(team_id) values(1)");
        select("davisbase_teams");
    }

    @Test
    public void insertDataWithUpperCase() {
        execute("create table faradaybase_teams (team_id int, team_name text)");
        execute("insert into faradaybase_teams (team_id, team_name) values (101, Faraday)");
        select("faradaybase_teams");
    }

    private Map<String, String> getDefaultDataMap() {
        Map<String, String> dataMap = new LinkedHashMap<>();
        dataMap.put("tinyint", "2");
        dataMap.put("smallint", "100");
        dataMap.put("int", "10033");
        dataMap.put("bigint", "8726463984");
        dataMap.put("float", "423.25");
        dataMap.put("double", "2.25432234");
        dataMap.put("year", "1988");
        dataMap.put("time", String.valueOf(LocalTime.now().get(ChronoField.MILLI_OF_DAY)));
        dataMap.put("datetime", "2027-03-04_10:10:10");
        dataMap.put("date", "2027-03-04_10:10:10");
        dataMap.put("text", "Everything is working!");
        return dataMap;
    }

    public void drop(String tableName) {
        execute(String.format("drop table %s", tableName));
    }

    public void select(String tableName) {
        execute(String.format("select * from %s", tableName));
    }

    public void create(String tableName, Set<String> columnTypes) {
        create(tableName, columnTypes.stream());
    }

    public void create(String tableName, String... columnTypes) {
        create(tableName, Arrays.stream(columnTypes));
    }

    public void create(String tableName, Stream<String> stream) {
        String createTableColumns = stream.map(c -> "col_" + c + " " + c).collect(Collectors.joining(","));
        String createTableQuery = String.format("create table %s (%s)", tableName, createTableColumns);
        execute(createTableQuery);
    }

    public void insert(String tableName, Map<String, String> dataMap) {
        String insertTableColumns = dataMap.keySet().stream().map(c -> "col_" + c).collect(Collectors.joining(","));
        String insertTableValues = dataMap.values().stream().collect(Collectors.joining(","));
        String insertTableQuery = String.format(String.format("insert into %s (%s) values (%s)", tableName, insertTableColumns, insertTableValues));
        execute(insertTableQuery);
    }

    public void execute(String query) {
        System.out.println(query);
        FaradayBaseApplication.parseUserCommand(query);
    }
}
