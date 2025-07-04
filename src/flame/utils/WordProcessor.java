package flame.utils;

import java.io.*;
import java.util.*;
import flame.external.PorterStemmer;

public class WordProcessor {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Usage: java WordProcessor <inputFile> <outputFile>");
            return;
        }

        String inputFile = args[0];
        String outputFile = args[1];

        // Load stop words
        Set<String> stopWords = loadStopWords();

        // Process words
        Set<String> processedWords = new HashSet<>(); // Use Set to avoid duplicates
        PorterStemmer stemmer = new PorterStemmer();

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

            String line;
            while ((line = reader.readLine()) != null) {
                String word = line.trim().toLowerCase();

                if (!stopWords.contains(word)) {
                    // Use PorterStemmer
                    for (char ch : word.toCharArray()) {
                        stemmer.add(ch);
                    }
                    stemmer.stem();
                    word = stemmer.toString();
                    processedWords.add(word);
                }
            }

            // Write processed words to the output file
            for (String word : processedWords) {
                writer.write(word);
                writer.newLine();
            }
        }
    }

    private static Set<String> loadStopWords() {
        return new HashSet<>(Arrays.asList("a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is", "it", "its", "of", "on", "that", "the", "to", "was", "were", "will", "with"));
    }
}