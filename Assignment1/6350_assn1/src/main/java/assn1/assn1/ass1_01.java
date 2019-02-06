package assn1.assn1;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class ass1_01 {
	public static void main(String[] args) {
		// download file;
		String[] books = { 
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2", 
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2",
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2",
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2", 
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2", 
				"http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2" 
				};
		String dirdst = args[0];

		for (String str : books) {
			String web = str;
			InputStream in = null;
			OutputStream out = null;

			// write to hdfs
			try {
				URL url = new URL(web);

				// ReadableByteChannel rbc = Channels.newChannel(website.openStream());
				// FileOutputStream fos = new FileOutputStream(FileName);
				// fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

				// download file and upload to HDFS
				in = new BufferedInputStream(url.openStream());
				
				Configuration conf = new Configuration();
				FileSystem fs = FileSystem.get(URI.create(dirdst), conf);
				String dst = dirdst + "/" + FilenameUtils.getName(url.getPath());
				// System.out.println(dst);

				Path dstPath = new Path(dst);

				out = fs.create(dstPath, new Progressable() {
					public void progress() {
						System.out.print(".");
					}
				});
				IOUtils.copyBytes(in, out, 4096, true);

				// decompress file and delete initial file;
				CompressionCodecFactory factory = new CompressionCodecFactory(conf);
				CompressionCodec codec = factory.getCodec(dstPath);
				if (codec == null) {
					System.err.println("No codec found for " + web);
					System.exit(1);
				}

				String outputUri = CompressionCodecFactory.removeSuffix(dst, codec.getDefaultExtension());

				in = codec.createInputStream(fs.open(dstPath));
				out = fs.create(new Path(outputUri));
				IOUtils.copyBytes(in, out, conf);
				
				fs.delete(dstPath, true);

			} catch (Exception e) {
				System.out.println(e.getMessage());
			} finally {
				IOUtils.closeStream(in);
				IOUtils.closeStream(out);
			}
		}
	}
}
