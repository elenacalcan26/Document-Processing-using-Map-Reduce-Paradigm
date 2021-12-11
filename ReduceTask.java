import java.util.ArrayList;
import java.util.HashMap;

public class ReduceTask extends Thread {

    private String document;
    private ArrayList<PartialSolution> partialSolutions;
    private int documentId; // numarul documentului de la intrare
    private float documentRang;
    private int longestLenWord; // retine lungimea celui mai lung cuvant
    private int longestLenWordApparitions; // retine numarul de aparitii a cuvintelor de lungime maxima
    private HashMap<Integer, Integer> apparitions; // salveaza combinarea dictionarelor de aparitii
    private ArrayList<String> longestWords;

    public ReduceTask(String document, ArrayList<PartialSolution> partialSolutions, int id) {
        this.document = document;
        this.partialSolutions = partialSolutions;
        this.documentId = id;
        apparitions = new HashMap<>();
        longestWords = new ArrayList<>();
    }

    public int getDocumentId() {
        return documentId;
    }

    public float getDocumentRang() {
        return documentRang;
    }

    public int getLongestLenWord() {
        return longestLenWord;
    }

    public int getLongestLenWordApparitions() {
        return longestLenWordApparitions;
    }

    public String getDocument() {
        return document;
    }

    /**
     * Combina hashmap-urile rezultate din task-ul de Map
     * Se calculeaza frecventa totala a lungimii cuvintelor din document
     */
    public void combineHashMaps() {
        for (PartialSolution partialSolution : partialSolutions) {
            // se ia rezultatul partial
            HashMap<Integer, Integer> partialApparitions = partialSolution.getApparitions();
            for (Integer len : partialApparitions.keySet()) {
                if (apparitions.containsKey(len)) {
                    // se ia frecventa lungimii din rezultatul partial
                    int value = partialApparitions.get(len);
                    // se ia frecventa totala calculata pana acum a lungimii
                    int lenApparitions = apparitions.get(len);
                    apparitions.put(len, value + lenApparitions);
                } else {
                    apparitions.put(len, partialApparitions.get(len));
                }
            }
        }
    }

    /**
     * Combina lista de cuvinte din urma task-ului de Map, salvand cuvintele de lungime maximala
     */
    public void combineLongestWords() {
        for (PartialSolution partialSolution : partialSolutions) {
            // se iau cuvintele din rezultatul partial
            ArrayList<String> words = partialSolution.getLongestWords();

            for (String word : words) {
                if (longestWords.isEmpty()) {
                    longestWords.add(word);
                } else if (word.length() > longestWords.get(0).length()) {
                    longestWords.clear();
                    longestWords.add(word);
                } else if (word.length() > longestWords.get(0).length()) {
                    longestWords.add(word);
                }
            }
        }

        longestLenWord = longestWords.get(0).length();
        longestLenWordApparitions = apparitions.get(longestLenWord);
    }

    /**
     * Realizeaza etapa de combinare
     */
    public void combine() {
        combineHashMaps();
        combineLongestWords();
    }

    /**
     * Calculeaza al n-lea element din sirul lui Fibonacci
     *
     * @param n index-ul elementului
     * @return elementul din sir
     */
    public int fib(int n) {
        if (n <= 1) {
            return n;
        }
        return fib(n - 1) + fib(n - 2);
    }

    /**
     * Realizeaza etapa de procesare in care se calculeaza rangul documentului
     */
    public void processRang() {
        float totalWordsNumber = 0.f;
        float sumFib = 0.f;

        for (Integer len : apparitions.keySet()) {
            sumFib += fib(len + 1) * apparitions.get(len);
            totalWordsNumber += apparitions.get(len);
        }
        documentRang = sumFib / totalWordsNumber;
    }

    @Override
    public void run() {
        combine();
        processRang();
    }
}
