package org.query.calc;

import it.unimi.dsi.fastutil.Pair;
import org.query.calc.dto.Row;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class FileService {
  //key = sum b + c
  //value = Row[2]

  //double as a key will create troubles


  public TreeMap<Double, List<Row>> precalculateAllSums(Path t2, Path t3) {
    TreeMap<Double, List<Row>> map = new TreeMap<>();

    final List<Row> r2 = parse(t2);
    final List<Row> r3 = parse(t3);

    for (Row row2 : r2) {
      for (Row row3 : r3) {
        double sum = row2.getKey() + row3.getKey();
        final List<Row> rows = map.get(sum);
        if (rows == null) {
          final ArrayList<Row> value = new ArrayList<>();
          value.add(row2);
          value.add(row3);
          map.put(sum, value);
        } else {
          rows.add(row2);
          rows.add(row3);
        }
      }
    }

    return map;
  }

  public List<Row> parse(Path path) {
    try {
      //1st is num of rows
      final List<String> lines = Files.readAllLines(path);
      final ArrayList<Row> rows = new ArrayList<>(Integer.parseInt(lines.get(0)));
      for (int i = 1; i < lines.size(); i++) {
        String line = lines.get(i);
        final String[] split = line.split(" ");
        final Row row = new Row(Double.parseDouble(split[0]), Double.parseDouble(split[1]));
        rows.add(row);
      }
      return rows;
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
      return Collections.emptyList();
    }
  }

  public void writeToFile(final Path output,
                          final Map<Double, Pair<Double, Double>> mapReduce,
                          final List<Pair<Double, Double>> values) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(output.toFile()))) {

      final String size = String.valueOf(Math.min(mapReduce.size(), 10));
      writer.write(size);

      for (int i = 0; i < values.size() && i < 10; i++) {
        Pair<Double, Double> pair = values.get(i);
        final String key = convertToString(pair.key());
        final String sumOutput = convertToString(pair.value());
        writer.newLine();
        writer.write(key + " " + sumOutput);
      }
    }
  }

  private String convertToString(Double value) {
    return value == (int) (double) value
        ? String.valueOf((int) (double) value)
        : String.format(Locale.US, "%.6f", value);
  }

}
