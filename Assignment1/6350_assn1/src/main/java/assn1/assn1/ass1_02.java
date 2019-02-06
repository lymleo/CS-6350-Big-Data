package assn1.assn1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ass1_02 {
	public static void main(String[] args) {
		String pdst = args[0];

		InputStream in = null;
		ZipInputStream zis = null;

		try {
			Configuration conf = new Configuration();
			// download zip file
			//!!!http is not working here;
			URL url = new URL("https://corpus.byu.edu/wikitext-samples/text.zip");
			in = new BufferedInputStream(url.openStream());
			zis = new ZipInputStream(in);

			// unzip file and save to text files according to file names;
			ZipEntry ze;
			while ((ze = zis.getNextEntry()) != null) {

				String dst = pdst + "/" +ze.getName();

				FileSystem fs = FileSystem.get(URI.create(dst), conf);

				Path dstPath = new Path(dst);
				OutputStream out = fs.create(dstPath, new Progressable() {
					public void progress() {
						System.out.print(".");
					}
				});

				IOUtils.copyBytes(zis, out, 4096, false);
				IOUtils.closeStream(out);

				// delete original file;
				fs.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(zis);
			IOUtils.closeStream(in);
		}
	}
}