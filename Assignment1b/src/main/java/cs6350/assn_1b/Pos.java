package cs6350.assn_1b;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Pos {

    HashMap<String,String> Lexicon = new HashMap<>();

    public Pos(String ref_file) {
        Path pt = new Path(ref_file);//Location of file in HDFS
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] items = line.split("@");
                Lexicon.put(items[0],items[1]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String getPos(String word) {
        String result = "others";
        String pos = Lexicon.get(word);
        if (pos != null) {
            switch (pos.charAt(0)) {
                case 'N':
                case 'p':
                case 'h':
                    result = "Noun";
                    break;
                case 'V':
                case 't':
                case 'i':
                    result = "Verb";
                    break;
                case 'r':
                    result = "Pronoun";
                    break;
                case 'A':
                    result = "Adjective";
                    break;
                case 'v':
                    result = "Adverb";
                    break;
                case 'C':
                    result = "Conjunction";
                    break;
                case 'P':
                    result = "Preposition";
                    break;
                case '!':
                    result = "Interjection";
                    break;
            }
        }
        return result;
    }

    public int isPlindromes(String word) {
        int length = word.length();
        String reverse = "";
        for (int i = length - 1; i >= 0; i--)
            reverse = reverse + word.charAt(i);

        if (word.equals(reverse)) return 1;
        return 0;
    }
}