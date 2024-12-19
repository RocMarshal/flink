package org.apache.flink;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;

public class CosineSimilarity {

    // 计算两个 Map 之间的余弦相似度，支持泛型类型
    public static <K> double cosineSimilarity(Map<K, Double> map1, Map<K, Double> map2) {
        double dotProduct = 0.0;
        double norm1 = 0.0;
        double norm2 = 0.0;

        // 计算点积和模长
        for (K key : map1.keySet()) {
            // 使用 getOrDefault 来处理缺失的键
            double value1 = map1.getOrDefault(key, 0.0);
            double value2 = map2.getOrDefault(key, 0.0);

            dotProduct += value1 * value2;
            norm1 += Math.pow(value1, 2);
        }

        // 计算第二个 Map 的模长
        for (double value : map2.values()) {
            norm2 += Math.pow(value, 2);
        }

        norm1 = Math.sqrt(norm1);
        norm2 = Math.sqrt(norm2);

        // 防止除以零
        if (norm1 == 0.0 || norm2 == 0.0) {
            return 0.0; // 处理零向量情况
        }

        return dotProduct / (norm1 * norm2);
    }


    // 计算皮尔逊相关系数
    public static double pearsonCorrelation(
            Map<String, Double> map1,
            Map<String, Double> map2) {
        // 获取两个Map中共有的键
        Set<String> commonKeys = map1.keySet();
        commonKeys.retainAll(map2.keySet());

        if (commonKeys.isEmpty()) {
            throw new IllegalArgumentException(
                    "Maps have no common keys to calculate Pearson correlation.");
        }

        // 计算均值
        double meanX = 0.0, meanY = 0.0;
        for (String key : commonKeys) {
            meanX += map1.get(key);
            meanY += map2.get(key);
        }
        meanX /= commonKeys.size();
        meanY /= commonKeys.size();

        // 计算分子和分母
        double numerator = 0.0;
        double denominatorX = 0.0;
        double denominatorY = 0.0;

        for (String key : commonKeys) {
            double xi = map1.get(key);
            double yi = map2.get(key);
            numerator += (xi - meanX) * (yi - meanY);
            denominatorX += Math.pow(xi - meanX, 2);
            denominatorY += Math.pow(yi - meanY, 2);
        }

        // 计算皮尔逊相关系数
        double denominator = Math.sqrt(denominatorX * denominatorY);
        if (denominator == 0) {
            throw new ArithmeticException("Denominator is zero, cannot compute Pearson correlation.");
        }

        return numerator / denominator;
    }

    public static void main(String[] args) {
        // 示例数据
        Map<String, Double> map1 = new HashMap<>();
        map1.put("a", 1.0);
        map1.put("b", 10.0);
        map1.put("c", 30.0);

        Map<String, Double> map2 = new HashMap<>();
        map2.put("a", 31.0);
        map2.put("b", 10.0);
        map2.put("c", 1.0);
//        map2.put("d", 5.0);  // 添加一个额外的键

        Map<String, Double> map3 = new HashMap<>();
        map3.put("a", 1.0);
        map3.put("b", 3.0);
        map3.put("c", 0.0);
//        map3.put("d", 0.0);

        // 计算皮尔逊相关系数
        System.out.println("P(m1,m2) " + pearsonCorrelation(map1, map2));//116
        System.out.println("P(m1,m3) " + pearsonCorrelation(map1, map3));//233
        System.out.println("P(m2,m3) " + pearsonCorrelation(map2, map3));//143


        // 计算并打印余弦相似度
        System.out.println("C(m1,m2) " + cosineSimilarity(map1, map2));
        System.out.println("C(m1,m3) " + cosineSimilarity(map1, map3));
        System.out.println("C(m2,m3) " + cosineSimilarity(map2, map3));
    }
}
