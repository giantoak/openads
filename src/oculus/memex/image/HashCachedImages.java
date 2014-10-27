package oculus.memex.image;

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.List;

import javax.imageio.ImageIO;

import oculus.xdataht.data.CsvParser;

public class HashCachedImages {
	public static BufferedImage getImage(String filename) {
		try {
			FileInputStream fis = new FileInputStream(filename);
			BufferedImage image = ImageIO.read(fis);
			fis.close();
			return image;
		} catch (Exception e) {
			System.err.println("Failed to read file: " + filename + " <" + e.getMessage() + ">");
		}
		return null;
	}

	public static void main(String[] args) {
		String infile = args[0];
		String cachedir = args[1];
		int firstID = 0;
		long starttime = System.currentTimeMillis();
		if (args.length>2) firstID = Integer.parseInt(args[2]);
		int processCount = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(infile));
			String line = null;
			while ((line = br.readLine()) != null) {
				List<String> strs = CsvParser.fsmParse(line);
				String imageid = strs.get(0);
				try {
					int id = Integer.parseInt(imageid);
					if (id<firstID) continue;
				} catch (Exception e) { System.out.println("Invalid imageid: <" + imageid + ">"); }
				String sha1 = strs.get(1);
				if (sha1==null) continue;
				try {
					BufferedImage image = getImage(cachedir+"/"+sha1+".jpg");
					String histogramHash = ImageHistogramHash.histogramHash(image);
					System.out.println(imageid + "," + sha1 + "," + histogramHash);
					processCount++;
				} catch (Exception e) {
					System.err.println("Failed on: " + imageid + "," + sha1);
				}
				if ( processCount%10000==0) {
					long endtime = System.currentTimeMillis();
					System.err.println("Processed: " + processCount + " Last 10000: " + (endtime-starttime) + "ms");
					starttime = endtime;
				}
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
}
