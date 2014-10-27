package oculus.memex.cloud.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.db.DBWritable;

import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.SqoopRecord;

@SuppressWarnings("deprecation")
public class ads_attributes extends SqoopRecord  implements DBWritable, Writable {
  private final int PROTOCOL_VERSION = 3;
  public int getClassFormatVersion() { return PROTOCOL_VERSION; }
  protected ResultSet __cur_result_set;
  private Long id;
  public Long get_id() {
    return id;
  }
  public void set_id(Long id) {
    this.id = id;
  }
  public ads_attributes with_id(Long id) {
    this.id = id;
    return this;
  }
  private Long ads_id;
  public Long get_ads_id() {
    return ads_id;
  }
  public void set_ads_id(Long ads_id) {
    this.ads_id = ads_id;
  }
  public ads_attributes with_ads_id(Long ads_id) {
    this.ads_id = ads_id;
    return this;
  }
  private String attribute;
  public String get_attribute() {
    return attribute;
  }
  public void set_attribute(String attribute) {
    this.attribute = attribute;
  }
  public ads_attributes with_attribute(String attribute) {
    this.attribute = attribute;
    return this;
  }
  private String value;
  public String get_value() {
    return value;
  }
  public void set_value(String value) {
    this.value = value;
  }
  public ads_attributes with_value(String value) {
    this.value = value;
    return this;
  }
  private Boolean extracted;
  public Boolean get_extracted() {
    return extracted;
  }
  public void set_extracted(Boolean extracted) {
    this.extracted = extracted;
  }
  public ads_attributes with_extracted(Boolean extracted) {
    this.extracted = extracted;
    return this;
  }
  private String extractedraw;
  public String get_extractedraw() {
    return extractedraw;
  }
  public void set_extractedraw(String extractedraw) {
    this.extractedraw = extractedraw;
  }
  public ads_attributes with_extractedraw(String extractedraw) {
    this.extractedraw = extractedraw;
    return this;
  }
  private java.sql.Timestamp modtime;
  public java.sql.Timestamp get_modtime() {
    return modtime;
  }
  public void set_modtime(java.sql.Timestamp modtime) {
    this.modtime = modtime;
  }
  public ads_attributes with_modtime(java.sql.Timestamp modtime) {
    this.modtime = modtime;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ads_attributes)) {
      return false;
    }
    ads_attributes that = (ads_attributes) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.ads_id == null ? that.ads_id == null : this.ads_id.equals(that.ads_id));
    equal = equal && (this.attribute == null ? that.attribute == null : this.attribute.equals(that.attribute));
    equal = equal && (this.value == null ? that.value == null : this.value.equals(that.value));
    equal = equal && (this.extracted == null ? that.extracted == null : this.extracted.equals(that.extracted));
    equal = equal && (this.extractedraw == null ? that.extractedraw == null : this.extractedraw.equals(that.extractedraw));
    equal = equal && (this.modtime == null ? that.modtime == null : this.modtime.equals(that.modtime));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.ads_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.attribute = JdbcWritableBridge.readString(3, __dbResults);
    this.value = JdbcWritableBridge.readString(4, __dbResults);
    this.extracted = JdbcWritableBridge.readBoolean(5, __dbResults);
    this.extractedraw = JdbcWritableBridge.readString(6, __dbResults);
    this.modtime = JdbcWritableBridge.readTimestamp(7, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(ads_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(attribute, 3 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(value, 4 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeBoolean(extracted, 5 + __off, -7, __dbStmt);
    JdbcWritableBridge.writeString(extractedraw, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(modtime, 7 + __off, 93, __dbStmt);
    return 7;
  }
  public void readFields(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.ads_id = null;
    } else {
    this.ads_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.attribute = null;
    } else {
    this.attribute = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.value = null;
    } else {
    this.value = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.extracted = null;
    } else {
    this.extracted = Boolean.valueOf(__dataIn.readBoolean());
    }
    if (__dataIn.readBoolean()) { 
        this.extractedraw = null;
    } else {
    this.extractedraw = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.modtime = null;
    } else {
    this.modtime = new Timestamp(__dataIn.readLong());
    this.modtime.setNanos(__dataIn.readInt());
    }
  }
  public void write(DataOutput __dataOut) throws IOException {
    if (null == this.id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.id);
    }
    if (null == this.ads_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.ads_id);
    }
    if (null == this.attribute) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, attribute);
    }
    if (null == this.value) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, value);
    }
    if (null == this.extracted) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeBoolean(this.extracted);
    }
    if (null == this.extractedraw) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, extractedraw);
    }
    if (null == this.modtime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.modtime.getTime());
    __dataOut.writeInt(this.modtime.getNanos());
    }
  }
  private static final DelimiterSet __outputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  public String toString() {
    return toString(__outputDelimiters, true);
  }
  public String toString(DelimiterSet delimiters) {
    return toString(delimiters, true);
  }
  public String toString(boolean useRecordDelim) {
    return toString(__outputDelimiters, useRecordDelim);
  }
  public String toString(DelimiterSet delimiters, boolean useRecordDelim) {
    StringBuilder __sb = new StringBuilder();
    char fieldDelim = delimiters.getFieldsTerminatedBy();
    __sb.append(FieldFormatter.escapeAndEnclose(id==null?"null":"" + id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(ads_id==null?"null":"" + ads_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(attribute==null?"null":attribute, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(value==null?"null":value, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(extracted==null?"null":"" + extracted, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(extractedraw==null?"null":extractedraw, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(modtime==null?"null":"" + modtime, delimiters));
    if (useRecordDelim) {
      __sb.append(delimiters.getLinesTerminatedBy());
    }
    return __sb.toString();
  }
  private static final DelimiterSet __inputDelimiters = new DelimiterSet((char) 44, (char) 10, (char) 0, (char) 0, false);
  private RecordParser __parser;
  public void parse(Text __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharSequence __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(byte [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(char [] __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(ByteBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  public void parse(CharBuffer __record) throws RecordParser.ParseError {
    if (null == this.__parser) {
      this.__parser = new RecordParser(__inputDelimiters);
    }
    List<String> __fields = this.__parser.parseRecord(__record);
    __loadFromFields(__fields);
  }

  private void __loadFromFields(List<String> fields) {
    Iterator<String> __it = fields.listIterator();
    String __cur_str = null;
    try {
    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.id = null; } else {
      this.id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.ads_id = null; } else {
      this.ads_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.attribute = null; } else {
      this.attribute = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.value = null; } else {
      this.value = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.extracted = null; } else {
      this.extracted = BooleanParser.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.extractedraw = null; } else {
      this.extractedraw = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.modtime = null; } else {
      this.modtime = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    ads_attributes o = (ads_attributes) super.clone();
    o.modtime = (o.modtime != null) ? (java.sql.Timestamp) o.modtime.clone() : null;
    return o;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("ads_id", this.ads_id);
    __sqoop$field_map.put("attribute", this.attribute);
    __sqoop$field_map.put("value", this.value);
    __sqoop$field_map.put("extracted", this.extracted);
    __sqoop$field_map.put("extractedraw", this.extractedraw);
    __sqoop$field_map.put("modtime", this.modtime);
    return __sqoop$field_map;
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Long) __fieldVal;
    }
    else    if ("ads_id".equals(__fieldName)) {
      this.ads_id = (Long) __fieldVal;
    }
    else    if ("attribute".equals(__fieldName)) {
      this.attribute = (String) __fieldVal;
    }
    else    if ("value".equals(__fieldName)) {
      this.value = (String) __fieldVal;
    }
    else    if ("extracted".equals(__fieldName)) {
      this.extracted = (Boolean) __fieldVal;
    }
    else    if ("extractedraw".equals(__fieldName)) {
      this.extractedraw = (String) __fieldVal;
    }
    else    if ("modtime".equals(__fieldName)) {
      this.modtime = (java.sql.Timestamp) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
}
