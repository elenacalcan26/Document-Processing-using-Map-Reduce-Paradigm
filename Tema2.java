import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class Tema2 {

    /**
     * retine toate solutiile partiale dupa procesarea fragmentelor dintr-un document
     */
    private static HashMap<String, ArrayList<PartialSolution>> partialSolutions = new HashMap<>();

    /**
     * retine pentru fiecare document offset-uri ce indica inceputul unui fragment
     */
    private static HashMap<String, ArrayList<Long>> fragmentsOffsets = new HashMap<>();

    private static ArrayList<String> documents = new ArrayList<>();
    private static int fragmentDim;
    private static int numDocuments;
    private static ArrayList<ReduceTask> listThreads = new ArrayList<>();

    public static void main(String[] args) throws IOException, ExecutionException,
            InterruptedException {

        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }

        int numThreads = Integer.parseInt(args[0]);
        readInput(args[1]);

        // initializare hashmap-uri
        for (String document : documents) {
            fragmentsOffsets.putIfAbsent(document, new ArrayList<>());
            partialSolutions.putIfAbsent(document, new ArrayList<>());
        }

        computeFragmentsDimension();

        // Se executa partea de Map pentru fiecare document
        for (String document : documents) {
            ExecutorService tpe = Executors.newFixedThreadPool(numThreads);

            CompletableFuture<ArrayList<PartialSolution>>
                    documentPartialSolutions = new CompletableFuture<>();
            long fileSize = Files.size(Path.of(document));
            ArrayList<Long> offsets = fragmentsOffsets.get(document);
            ArrayList<PartialSolution> documentPartialSolution = new ArrayList<>();

            MapTask task = new MapTask(tpe, documentPartialSolutions, document, fileSize,
                    (long) fragmentDim, offsets, 0, documentPartialSolution);

            tpe.submit(task);

            ArrayList<PartialSolution> result = documentPartialSolutions.get();
            partialSolutions.put(document, result);
        }

        ReduceTask[] threads = new ReduceTask[numDocuments];

        // se executa partea de Reduce pentru fiecare document
        for (int i = 0; i < numDocuments; i++) {
            threads[i] = new ReduceTask(documents.get(i),
                    partialSolutions.get(documents.get(i)), i);
            threads[i].start();
        }

        for (int i = 0; i < numDocuments; i++) {
            threads[i].join();
            listThreads.add(threads[i]);
        }

        sortThreadsByDocumentRang();
        writeResults(args[2]);

    }

    public static void readInput(String file) throws IOException {
        File inputFile = new File(file);
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        String line;
        fragmentDim = Integer.parseInt(br.readLine());
        numDocuments = Integer.parseInt(br.readLine());
        while ((line = br.readLine()) != null) {
            documents.add(line);
        }
    }

    public static void writeResults(String file) throws IOException {
        File outputFile = new File(file);
        BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile));
        for (ReduceTask task : listThreads) {

            String[] documentPath = (task.getDocument()).split("/");
            String document = documentPath[documentPath.length - 1];
            float rang = task.getDocumentRang();
            int lenWord = task.getLongestLenWord();
            int lenWordApp = task.getLongestLenWordApparitions();

            bw.write(document + "," + String.format("%.2f", rang) + "," +
                    lenWord + "," + lenWordApp + "\n");
        }
        bw.flush();
        bw.close();
    }

    /**
     * Imparte documentele in fragmente
     *
     * @throws IOException
     */
    public static void computeFragmentsDimension() throws IOException {
        for (String document : documents) {
            long fileSize = Files.size(Path.of(document));
            long step = 0;

            while (step < fileSize) {
                fragmentsOffsets.get(document).add(step);
                step += fragmentDim;
            }

        }
    }

    /**
     * Sorteaza thread-urile dupa rang-ul calculat pentru fiecare document asignat
     */
    public static void sortThreadsByDocumentRang() {
        Comparator<ReduceTask> sortByRang = new Comparator<ReduceTask>() {
            @Override
            public int compare(ReduceTask o1, ReduceTask o2) {
                if (o2.getDocumentRang() > o1.getDocumentRang()) {
                    return 1;
                } else if (o2.getDocumentRang() < o1.getDocumentRang()) {
                    return -1;
                }
                return o1.getDocumentId() - o2.getDocumentId();
            }
        };

        Collections.sort(listThreads, sortByRang);
    }
}
