/**
 * Copyright (c) 2013 Oculus Info Inc.
 * http://www.oculusinfo.com/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package oculus.xdataht.data;
import java.util.ArrayList;
import java.util.List;

public class CsvParser {	
	public static List<String> fsmParse(String input) {
		ArrayList<String> result = new ArrayList<String>();
		int startChar = 0;
		int endChar = 0;
		boolean inString = false;
		while(endChar<input.length()) {
			char c = input.charAt(endChar);
			if (inString) {
				endChar++;
				if (c=='"') {
					if (endChar<input.length()&&input.charAt(endChar)=='"') {
						endChar++;
					} else {
						inString = false;
					}
				}
			} else {
				endChar++;
				if (c==',') {
					result.add(input.substring(startChar, endChar-1));
					startChar = endChar;
				} else if (c=='"') {
					inString = true;
				}
			}
		}
		if (startChar != endChar) {
			result.add(input.substring(startChar, endChar));
		}
		return result;
	}
}
