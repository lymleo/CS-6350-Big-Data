import java.io.IOException;
import java.io.PrintWriter;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class WordCount {
	
	
	public static void main (String args[]) throws IOException{
		//get the content of text file;
		URL url = new URL("http://www.gutenberg.org/files/98/98-0.txt");
		Scanner s = new Scanner(url.openStream());
		
		//count occurrence
		Map<String, Integer> m = new HashMap<>();
		while(s.hasNext()) {
			String word = s.next();
			String[] words = word.replaceAll("[^a-zA-Z ]", "").toLowerCase().split("\\s+");
			for(String str : words) {
				if(str != null &&  !str.isEmpty()) {
					if(!m.containsKey(str)) 
						m.put(str, 1);
					else
						m.put(str, m.get(str)+1);
				}
			}		
		}
		
		
		//stores counts in an decreasing order
		List<String> l = new ArrayList<String>(m.keySet());
		final Map<String, Integer> map = m;
		

		Comparator<String> cmp = new Comparator<String>() {
			@Override
			public int compare(String k1, String k2) {
				Integer v1 = map.get(k1);
				Integer v2 = map.get(k2);
				return v1.compareTo(v2);
			}
		};
		Collections.sort(l, Collections.reverseOrder(cmp));
		
		//Write text file named "yxl160531Part2.txt"
		PrintWriter txt = new PrintWriter("yxl160531Part2.txt", "UTF-8");
		for(String str: l) {
			txt.println(str + " " + map.get(str));
		}
	}

}
