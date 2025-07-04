package flame.jobs;

import flame.kvs.KVSClient;
import flame.flame.FlameContext;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

// Didn't use this class since better to not have such a big lexicon for performance reasons. Better to just compare against an in-memory list of words

public class IndexInitializer {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        System.out.println("Starting index initialization...");

        String indexName = "pt-index";
        String wordFile = "processed-words.txt";

        // Access the KVS client
        KVSClient kvsClient = flameContext.getKVS();

        // Ensure file exists
        File vocabFile = new File(wordFile);
        if (!vocabFile.exists()) {
            System.err.println("Error: Could not find file: " + wordFile);
            return;
        }

        // Load words and filter out any invalid entries
        List<String> words = Files.lines(vocabFile.toPath())
                .map(String::trim)
                .filter(word -> !word.isEmpty())
                .distinct()
                .collect(Collectors.toList());

        // Populate the index
        for (String word : words) {
            kvsClient.put(indexName, word, "urls", "");
        }

        System.out.println("Index initialization completed. Total words added: " + words.size());
    }
}
