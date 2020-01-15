import java.io.Serializable;

public class WordEvent implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String words;

    public WordEvent(){}

    public WordEvent(String words){
        this.words = words;
    }

    public String getWords() {
        return words;
    }
    public void setWords(String words) {
        this.words = words;
    }

    @Override
    public String toString() {
        return words;
    }
}