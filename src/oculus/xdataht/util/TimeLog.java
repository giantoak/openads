package oculus.xdataht.util;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Stack;

public class TimeLog {
	private Stack<Pair<String,Long>> _timingStack = new Stack<Pair<String,Long>>();
	private static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	public void pushTime(String operationName) {
		try {
			long time = System.currentTimeMillis();
			for (int i=0; i<_timingStack.size(); i++) System.out.print("\t");
			System.out.println("BEGIN: " + operationName + (_timingStack.size()==0?" " + DATE_FORMAT.format(new Date()):""));
			_timingStack.push(new Pair<String,Long>(operationName, time));
		} catch (Exception e) {e.printStackTrace();}
	}
	public void popTime() {
		try {
			long time = System.currentTimeMillis();
			Pair<String,Long> item = _timingStack.pop();
			for (int i=0; i<_timingStack.size(); i++) System.out.print("\t");
			System.out.println("END: " + item.getFirst() + ": " + (time-item.getSecond()) + "ms");
		} catch (Exception e) {e.printStackTrace();}
	}
}
