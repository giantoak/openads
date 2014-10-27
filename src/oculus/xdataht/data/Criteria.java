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

import org.json.JSONException;
import org.json.JSONObject;

public class Criteria {
	public enum OPERATOR {
		EQUALS,
		CONTAINS
	};

	public String attribute;
	public OPERATOR operator;
	public String value;
	
	public Criteria(JSONObject jsonObject) throws JSONException {
		attribute = jsonObject.getString("attribute");
		String operatorStr = jsonObject.getString("operator");
		if (operatorStr.equalsIgnoreCase("equals")) {
			operator = OPERATOR.EQUALS; 
		} else if (operatorStr.equalsIgnoreCase("contains")) {
			operator = OPERATOR.CONTAINS; 
		} else {
			operator = OPERATOR.EQUALS; 
		}
		value = jsonObject.getString("value");
	}

	public boolean matches(DataRow test) {
		String testVal = test.get(attribute);
		if (testVal==null) return false;
		switch (operator) {
		case EQUALS:
			return testVal.equalsIgnoreCase(value);
		case CONTAINS:
			return testVal.contains(value);
		default:
			return true;
		}
	}
	
}
