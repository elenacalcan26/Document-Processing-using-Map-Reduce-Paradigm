import java.util.ArrayList;
import java.util.HashMap;

/**
 * Clasa salveaza rezultatele in urma procesarii unui fragment din document
 * in etapa de Map
 */

public class PartialSolution {

    /**
     * retine lungimea cuvintelor gasite din fragment cu frecventa ei
     */
    private HashMap<Integer, Integer> apparitions;

    /**
     * lista de cuvinte ce au lungime maxima din fragment
     */
    private ArrayList<String> longestWords;

    public PartialSolution(HashMap<Integer, Integer> apparitions, ArrayList<String> longestWords) {
        this.apparitions = apparitions;
        this.longestWords = longestWords;
    }

    public HashMap<Integer, Integer> getApparitions() {
        return apparitions;
    }

    public ArrayList<String> getLongestWords() {
        return longestWords;
    }

    @Override
    public String toString() {
        return "PartialSolution{" +
                "apparitions=" + apparitions +
                ", longestWords=" + longestWords +
                '}';
    }
}
