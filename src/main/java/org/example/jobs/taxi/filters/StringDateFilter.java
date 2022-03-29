package org.example.jobs.taxi.filters;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class StringDateFilter implements SerializableFunction<TableRow, Boolean> {

    Boolean isValidDateTime(String dateStr) {
        LocalDate dt = LocalDate.parse(dateStr.substring(0, 10));
        LocalDate startDate = LocalDate.of(2016, 1, 1);
        LocalDate endDate = LocalDate.of(2017, 1, 1);
        return dt.isBefore(endDate) && dt.isAfter(startDate.minusDays(1));
    }

    @Override
    public Boolean apply(TableRow row) {
        String dateStr = row.get("pickup_datetime").toString();
        return isValidDateTime((dateStr));
    }
}
