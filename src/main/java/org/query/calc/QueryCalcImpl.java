package org.query.calc;

import it.unimi.dsi.fastutil.Pair;
import org.query.calc.dto.Row;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class QueryCalcImpl implements QueryCalc {
  @Override
  public void select(Path t1, Path t2, Path t3, Path output) throws IOException {
    // - t1 is a file contains table "t1" with two columns "a" and "x". First line is a number of rows, then each
    //  line contains exactly one row, that contains two numbers parsable by Double.parse(): value for column a and
    //  x respectively.See test resources for examples.
    // - t2 is a file contains table "t2" with columns "b" and "y". Same format.
    // - t3 is a file contains table "t3" with columns "c" and "z". Same format.
    // - output is table stored in the same format: first line is a number of rows, then each line is one row that
    //  contains two numbers: value for column a and s.
    //
    // Number of rows of all three tables lays in range [0, 1_000_000].
    // It's guaranteed that full content of all three tables fits into RAM.
    // It's guaranteed that full outer join of at least one pair (t1xt2 or t2xt3 or t1xt3) of tables can fit into RAM.
    //
    // TODO: Implement following query, put a reasonable effort into making it efficient from perspective of
    //  computation time, memory usage and resource utilization (in that exact order). You are free to use any lib
    //  from a maven central.
    //
    // SELECT a, SUM(x * y * z) AS s FROM
    // t1 LEFT JOIN (SELECT * FROM t2 JOIN t3) AS t
    // ON a < b + c
    // GROUP BY a
    // STABLE ORDER BY s DESC
    // LIMIT 10;
    //
    // Note: STABLE is not a standard SQL command. It means that you should preserve the original order.
    // In this context it means, that in case of tie on s-value you should prefer value of a, with a lower row number.
    // In case multiple occurrences, you may assume that group has a row number of the first occurrence.


    final FileService fileService = new FileService();
    //O(N^2)
    //could be improved. for example. we could sort and precalculate only MIN and MAX values.
    //
    //if required. we could sort. then use binary search(tree) to find from where we could start our all possible
    // sums (in next steps)
    final TreeMap<Double, List<Row>> treeMap = fileService.precalculateAllSums(t2, t3);

    final List<Row> parse = fileService.parse(t1);

    //O(N*logN)
    final Map<Double, Pair<Double, Double>> mapReduce = mapReduce(treeMap, parse);

    //O(N*logN)
    final List<Pair<Double, Double>> values = sortResult(mapReduce);

    fileService.writeToFile(output, mapReduce, values);
  }


  private List<Pair<Double, Double>> sortResult(Map<Double, Pair<Double, Double>> mapReduce) {
    final List<Pair<Double, Double>> values = new ArrayList<>(mapReduce.values());
    values.sort((o1, o2) -> {
      final double diff = (o2.value() - o1.value());
      if (diff == 0) {
        final double diff2 = (o2.key() - o1.key());
        if (diff2 == 0) {
          return 0;
        } else if (diff2 < 0) {
          return -1;
        } else {
          return 1;
        }
      } else if (diff < 0) {
        return -1;
      } else {
        return 1;
      }
    });
    return values;
  }

  private Map<Double, Pair<Double, Double>> mapReduce(final TreeMap<Double, List<Row>> treeMap,
                                                      final List<Row> parse) {
    final Map<Double, Pair<Double, Double>> mapReduce = new HashMap<>();

    for (Row row : parse) {
      //logN - find
      // O(N) - iterate
      final NavigableMap<Double, List<Row>> doubleListNavigableMap = treeMap.tailMap(row.getKey(), false);
      double sum = 0;
      // double O(N) to O(N^2)
      for (Map.Entry<Double, List<Row>> entry : doubleListNavigableMap.entrySet()) {
        List<Row> rows = entry.getValue();
        for (int i = 0; i < rows.size(); i += 2) {
          Row innerRow = rows.get(i);
          Row innerRow2 = rows.get(i + 1);
          sum += row.getValue() * innerRow.getValue() * innerRow2.getValue();
        }
      }


      final Pair<Double, Double> pair;
      if (mapReduce.containsKey(row.getKey())) {
        final Pair<Double, Double> mapPair = mapReduce.get(row.getKey());
        pair = Pair.of(mapPair.key(), mapPair.value() + sum);
      } else {
        pair = Pair.of(row.getKey(), sum);
      }
      mapReduce.put(row.getKey(), pair);
    }

    return mapReduce;
  }
}
