package org.utd.faradaybase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class test {
    public static void main(String[] args) {
        String queryString = "INSERT INTO faradaybase_teams (team_id, team_name) VALUES (101, Faraday)";
        System.out.println(queryString.split("(?i)values")[1]);
    }
}
