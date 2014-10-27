package oculus.xdataht.image;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.imageio.ImageIO;

public class ImageHistogram {

	public static BufferedImage getImage(String urlToRead) {
		URL url;
		HttpURLConnection conn;
		try {
			url = new URL(urlToRead);
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			return ImageIO.read(conn.getInputStream());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	static int COLOR_DEPTH = 4;
	static int COLOR_DIVISOR = 256/COLOR_DEPTH;
	static int DISTINCT_COLORS = (int)Math.pow(COLOR_DEPTH,3);
	
	public static String getHash(BufferedImage img) {
		Raster raster = img.getData();
		int h = raster.getHeight();
		int w = raster.getWidth();
		int pixels = w*h;
		System.out.println("Image size: (" + w + "," + h + ") " + pixels);
		int[] colors = new int[pixels*3];
		raster.getPixels(0, 0, w, h, colors);
		int[] counts = new int[DISTINCT_COLORS];
		for (int i=0; i<DISTINCT_COLORS; i++) counts[i] = 0;
		for (int i=0; i<w*h; i++) {
			int r = colors[i*3]/COLOR_DIVISOR;
			r = Math.min(r, COLOR_DEPTH-1);
			int g = (colors[i*3+1])/COLOR_DIVISOR;
			g = Math.min(g, COLOR_DEPTH-1);
			int b = (colors[i*3+2])/COLOR_DIVISOR;
			b = Math.min(b, COLOR_DEPTH-1);
			int truncColor = (r*COLOR_DEPTH+g)*COLOR_DEPTH+b;
			counts[truncColor]++;
		}
		String result = "";
		for (int i=0; i<DISTINCT_COLORS; i++) {
			result += (int)Math.ceil((counts[i]*DISTINCT_COLORS)/pixels) + ".";
		}
		return result;
	}

	public static void testUrl(String url) {
//		String testUrl = "http://upload.wikimedia.org/wikipedia/commons/4/41/Mata_Hari_2.jpg";
		BufferedImage img = getImage(url);
		String hash = getHash(img);
		System.out.println("Hash:" + hash);
	}
	
	public static void main(String[] args) {
		testUrl("http://upload.wikimedia.org/wikipedia/commons/a/a0/Baath_Party_founder_Michel_Aflaq_with_Iraqi_President_Saddam_Hussein_in_1988.jpg");
		testUrl("http://upload.wikimedia.org/wikipedia/commons/5/5c/Double-alaskan-rainbow.jpg");
		try {
			BufferedImage img = ImageIO.read(new File("C:/Users/eric/Desktop/Mata_Hari_3.jpg"));
			String hash = getHash(img);
			System.out.println("Hash:" + hash);
			img = ImageIO.read(new File("C:/Users/eric/Desktop/Mata_Hari_2.jpg"));
			hash = getHash(img);
			System.out.println("Hash:" + hash);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
