package org.example.jobsTest;
import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

public class TestUtils {

    static  Boolean isValidDateTime(String dateStr) {

            System.out.println(dateStr.substring(0, 10));
            LocalDate dt = LocalDate.parse(dateStr.substring(0, 10));
            LocalDate startDate = LocalDate.of(2016, 1, 1);
            LocalDate endDate = LocalDate.of(2017, 1, 1);
            return dt.isBefore(endDate) && dt.isAfter(startDate.minusDays(1));
    }
    public static void main(String[] args) {
        System.out.println(isValidDateTime("2017-12-19 00:16:05 UTC"));
    }
}
