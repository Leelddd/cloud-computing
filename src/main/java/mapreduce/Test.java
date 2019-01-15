package mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class Test {
    public static void main(String[] args) throws IOException {
        Map<String, Set<String>> map = new HashMap<>();
        for (int i = 1; i <= 4; i++) {
            BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\Leeld\\Documents\\dataset\\twitter\\twitter" + i + ".txt"));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] a = line.split(" ");
                if (map.containsKey(a[0])) {
                    map.get(a[0]).add(a[1]);
                } else {
                    Set<String> set = new HashSet<>();
                    set.add(a[1]);
                    map.put(a[0], set);
                }
            }
        }

        List<String> list = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : map.entrySet()) {
            for (String s: entry.getValue()){
                if (map.containsKey(s) && map.get(s).contains(entry.getKey())){
                    list.add(entry.getKey() + " " + s);
                }
            }
        }
        System.out.println(list.size());
    }
}
