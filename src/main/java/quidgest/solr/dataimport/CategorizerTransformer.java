package quidgest.solr.dataimport;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.Transformer;

public class CategorizerTransformer extends Transformer {

	private Tuple[] rules;
	private Map<String, List<String>> paths;

	@Override
	public Object transformRow(Map<String, Object> row, Context context) {
		if (rules == null)
			init(context);

		// Match directories with rules to get tags
		String directory = ((String) row.get("fileDirReplaced"));
		List<String> arr = new ArrayList<String>();
		Set<String> usedPaths = new HashSet<String>();
		for (Tuple t : rules) {
			if (!usedPaths.contains(t.path)
					&& t.path.length() <= directory.length()
					&& directory.startsWith(t.path)) {
				arr.addAll(t.tags);
				usedPaths.addAll(paths.get(t.path));
			}
		}

		// If does not have any match, than add the default value
		if (arr.size() == 0)
			arr.add("Outros");

		row.put("fileDirTransformed", arr);

		return row;
	}

	public void init(Context context) {
		System.out.println("Initing the paths and the rules");

		List<Map<String, String>> fields = context.getAllEntityFields();

		Scanner in = null;
		for (Map<String, String> field : fields) {
			String file = field.get("categories");
			if (file != null && !file.equals(""))
				try {
					in = new Scanner(new FileInputStream(file));
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
		}

		// Reads the rules
		List<Tuple> r = new ArrayList<Tuple>();
		paths = new HashMap<String, List<String>>();
		while (in.hasNextLine()) {
			String line = in.nextLine();
			String[] args = line.split(" => ");
			String lhs = args[0];
			String[] rhs = stripExtraWhitespaces(args[1].split(","));
			addPath(lhs);
			Tuple t = new Tuple(lhs, Arrays.asList(rhs));
			r.add(t);
		}

		rules = new Tuple[r.size()];
		r.toArray(rules);
		Arrays.sort(rules);
	}

	private String[] stripExtraWhitespaces(String[] splits) {
		for (int i = 0; i < splits.length; i++)
			splits[i] = splits[i].trim();

		return splits;
	}

	private void addPath(String originalPath) {
		String[] partialPaths = originalPath.split("/");
		String path = "";
		List<String> l = new ArrayList<String>();
		for (int i = 0; i < partialPaths.length; i++) {
			path += partialPaths[i];
			l.add(path);
			path += "/";
		}

		paths.put(originalPath, l);
	}

	protected class Tuple implements Comparable<Tuple> {
		private String path;
		private List<String> tags;

		public Tuple(String path, List<String> tags) {
			this.path = path;
			this.tags = tags;
		}

		public int compareTo(Tuple o) {
			int result = path.length() - o.path.length();
			if (result > 0)
				return -1;
			else if (result < 0)
				return 1;
			else
				return 0;
		}

		public String toString() {
			return "Length:" + path.length();
		}
	}
}
