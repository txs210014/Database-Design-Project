# FARADAYBASE SQLite Database
This project demonstrates the fundamentals of a database desiged using BTree and B+ Trees.

### Project Startup
1. This project is a maven-build based Project. Run `mvn clean install` in the project base folder.
2. Once the build is successful and targets are generated, use below command to launch the application - 
    `java -jar target/faradaybase-1.0-jar-with-dependencies.jar`
3. This will launch the Faradaybase SQLlite prompt to run.

### Commands to run
- Show all tables;
```
show tables;
```

- Show Catalog Tables
```
select * from faradaybase_tables;
select * from faradaybase_columns;
```

- Creating table
```
CREATE TABLE faradaybase_teams (team_id INT, team_name TEXT);
```

- Inserting into table
```
INSERT INTO faradaybase_teams (team_id, team_name) VALUES (101, Faraday);
INSERT INTO faradaybase_teams (team_id, team_name) VALUES (102, Rutherford);
```

- Selecting the data
```
SELECT * FROM faradaybase_teams WHERE team_name = 'Faraday';
```

- Updating the data
```
UPDATE faradaybase_teams SET team_name = Faraday2 WHERE team_id = 101;
```

- Delete from table
```
DELETE FROM table faradaybase_teams where team_id = 102;
```

- Creating index on table
```
CREATE INDEX ON faradaybase_teams (team_id);
```

- Drop table 
```
drop table faradaybase_teams;
```

- Exit from system
```
exit;
```
