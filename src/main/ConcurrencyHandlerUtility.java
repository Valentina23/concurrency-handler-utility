package main;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConcurrencyHandlerUtility {

    public static void main(String[] args) throws Exception {

        Executor threadPool = Executors.newCachedThreadPool();

        List<Future<Map<String, Set<String>>>> futures = new ArrayList<>();
        for (String filePath : args) {
            Future<Map<String, Set<String>>> future = CompletableFuture.supplyAsync(
                    () -> handleFile(filePath),
                    threadPool
            );

            futures.add(future);
        }

        waitForResults(futures);

        Map<String, Set<String>> resultMap = mergeResults(futures);

        writeToFile(resultMap);

        ((ExecutorService) threadPool).shutdown();
    }

    static Map<String, Set<String>> handleFile(String filePath) {
        Map<String, Set<String>> result = new LinkedHashMap<>();

        try {
            Stream<String> linesStream = Files.lines(Paths.get(filePath));

            List<String> lines = linesStream.filter((line) -> !line.isEmpty()).collect(Collectors.toList());

            String[] columnHeaders = lines.get(0).split(";");
            lines.remove(0);

            for (String columnHeader : columnHeaders) {
                result.put(columnHeader, new HashSet<>());
            }

            for (String line : lines) {
                String[] words = line.split(";");

                Iterator<Set<String>> columnIterator = result.values().iterator();

                for (String word : words) {
                    if (!columnIterator.hasNext()) {
                        break;
                    }

                    Set<String> column = columnIterator.next();

                    column.add(word);
                }
            }

        } catch (IOException e) {
            System.out.println("Error IO " + filePath);
        }

        return result;
    }

    static void waitForResults(List<Future<Map<String, Set<String>>>> futures) {
        CompletableFuture[] cfs = futures.toArray(
                new CompletableFuture[futures.size()]
        );

        CompletableFuture.allOf(cfs).join();
    }

    static Map<String, Set<String>> mergeResults(List<Future<Map<String, Set<String>>>> futures) throws Exception {
        Map<String, Set<String>> resultMap = new HashMap<>();

        for (Future<Map<String, Set<String>>> future : futures) {
            Map<String, Set<String>> interRes = future.get();

            for (Map.Entry<String, Set<String>> entry : interRes.entrySet()) {
                String key = entry.getKey();
                Set<String> interResSet = entry.getValue();

                Set<String> resultSet = resultMap.get(key);

                if (resultSet == null) {
                    resultSet = new HashSet<>();
                    resultMap.put(key, resultSet);
                }

                resultSet.addAll(interResSet);
            }
        }

        return resultMap;
    }

    static void writeToFile(Map<String, Set<String>> resultMap) throws IOException {
        for (Map.Entry<String, Set<String>> entry : resultMap.entrySet()) {
            String fileName = String.valueOf(entry.getKey()) + ".csv";

            Files.write(Paths.get(fileName), entry.getValue());
        }
    }
}
