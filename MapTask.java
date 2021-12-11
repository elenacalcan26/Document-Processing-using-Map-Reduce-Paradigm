import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class MapTask implements Runnable {

    private ExecutorService tpe;
    private CompletableFuture<ArrayList<PartialSolution>> partialSolutions;
    private String filename;
    private long fileDim;
    private Long fragmentDim;
    private ArrayList<Long> documentOffsets;
    private int step;
    private ArrayList<PartialSolution> fragmentSolution;

    /**
     * dictionarul cu lungimile cuvintelor gasite din fragmentul curent si aparitiile lor
     */
    private HashMap<Integer, Integer> apparitions;

    /**
     * cuvintele de lungime maxima din fragmentul curent
     */
    private ArrayList<String> fragmentLongestWord;
    private String delimiters = ";:/?~.,><~`[]{}()!@#$%^&-+'=*|" + '\r' + '\n' + '\t' + ' ' + '"';

    public MapTask(ExecutorService tpe,
                   CompletableFuture<ArrayList<PartialSolution>> partialSolutions,
                   String filename, long fileDim, Long fragmentDim, ArrayList<Long> documentOffsets,
                   int step, ArrayList<PartialSolution> fragmentSolution) {
        this.tpe = tpe;
        this.partialSolutions = partialSolutions;
        this.filename = filename;
        this.fileDim = fileDim;
        this.fragmentDim = fragmentDim;
        this.documentOffsets = documentOffsets;
        this.step = step;
        this.fragmentSolution = fragmentSolution;
        apparitions = new HashMap<>();
        fragmentLongestWord = new ArrayList<>();
    }

    /**
     * Adauga lungimea unui cuvant citit din fragment si actualizeaza frecventa lungimii
     *
     * @param word cuvantul adaugat
     */
    public void increaseApparitions(String word) {
        if (word != "") {
            int len = word.length();
            if (apparitions.containsKey(len)) {

                apparitions.put(len, apparitions.get(len) + 1);
            } else {
                apparitions.putIfAbsent(len, 1);
            }
        }
    }

    /**
     * Verifica daca noul cuvant gasit in fragment este cel mai lung si il salveaza
     * intr-un ArrayList cand este cazul
     *
     * @param word cuvant citit
     */
    public void addLongestWord(String word) {

        if (word != "" && delimiters.indexOf(word) < 0) {

            // se verifica lungimile cuvintelor pentru a determina care este mai lung
            if (fragmentLongestWord.isEmpty()) {
                fragmentLongestWord.add(word);
            } else if (word.length() > fragmentLongestWord.get(0).length()) {
                fragmentLongestWord.clear();
                fragmentLongestWord.add(word);
            } else if (word.length() == fragmentLongestWord.get(0).length()) {
                fragmentLongestWord.add(word);
            }
        }
    }

    /**
     * Calculeaza o noua pozitie de inceput a fragmentului din document cand fragmentul inecpe
     * in mijlocul unui cuvant
     * Pozitia este incrementata pana cand se gaseste un delimitator
     *
     * @param offset pozitia de inceput a fragmentului din document
     * @return vechiul sau un nou offset dupa caz
     * @throws IOException
     */
    public long fragmentStartOffset(long offset) throws IOException {
        char dummy, firstFragmentChar;
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        raf.seek(offset - 1); // se pozitioneaza la ultimul caracter al fragmentului anterior
        dummy = (char) raf.read();
        firstFragmentChar = (char) raf.read();

        // se verifica dac fragmentul incepe in mijlocul unui cuvant
        if (delimiters.indexOf(dummy) < 0 && delimiters.indexOf(firstFragmentChar) < 0) {
            dummy = firstFragmentChar;

            // se citeste in gol, crescand astfel offset-ul
            while (delimiters.indexOf(dummy) < 0 && offset < fileDim) {
                dummy = (char) raf.read();
                offset++;
            }
        }
        return offset;
    }

    /**
     * Se citeste caracter cu caracter fragmentul din fisier si prelucreaza cuvintele citite
     *
     * @param offset pozitia de start a fragmentului
     * @throws IOException
     */
    public void readFragment(long offset) throws IOException {
        RandomAccessFile raf = new RandomAccessFile(filename, "r");
        String word = "";
        char readCh = 0;
        long i;
        long endFragment = documentOffsets.get(step) + fragmentDim; // sfarsitul fragmentului

        raf.seek(offset); // se duce la inceputul fragmentului din fisier

        for (i = offset; i < endFragment; i++) {

            if (i == fileDim) {
                // s-a ajuns la finalul fisierului => nu se mai trimit task-uri in pool
                increaseApparitions(word);
                addLongestWord(word);
                fragmentSolution.add(new PartialSolution(apparitions, fragmentLongestWord));
                partialSolutions.complete(fragmentSolution);
                tpe.shutdown();
                return;
            }

            readCh = (char) raf.read();

            // se verifica daca s-a citit o litera
            if (delimiters.indexOf(readCh) < 0) {
                // s-a citit o litera
                word += readCh;
            } else {
                // delimtator si se prelucreaza cuvantul citit
                increaseApparitions(word);
                addLongestWord(word);
                word = "";
            }
        }

        // se verifica ultimul caracter citit
        if (delimiters.indexOf(readCh) < 0 && i != fileDim) {
            readCh = (char) raf.read();

            // fragmentul se termina in mijlocul unui cuvant
            while (delimiters.indexOf(readCh) < 0 && i < fileDim) {
                // se citesc urmatoarele caractere ale cuvantului
                word += readCh;
                readCh = (char) raf.read();
                i++;
            }
            // se prelucreaza ultimul cuvant din fragment
            increaseApparitions(word);
            addLongestWord(word);
        }

        // se trimite un nou task in pool
        fragmentSolution.add(new PartialSolution(apparitions, fragmentLongestWord));
        MapTask task = new MapTask(tpe, partialSolutions, filename, fileDim, fragmentDim,
                documentOffsets, step + 1, fragmentSolution);
        tpe.submit(task);
    }

    @Override
    public void run() {
        long offset = documentOffsets.get(step);
        if (offset > 0) {
            try {
                offset = fragmentStartOffset(offset);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            readFragment(offset);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
