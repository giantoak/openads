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
import org.apache.sqoop.lib.FieldFormatter;
import org.apache.sqoop.lib.JdbcWritableBridge;
import org.apache.sqoop.lib.SqoopRecord;

import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.RecordParser;

@SuppressWarnings("deprecation")
public class ads extends SqoopRecord  implements DBWritable, Writable {
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
  public ads with_id(Long id) {
    this.id = id;
    return this;
  }
  private Long first_id;
  public Long get_first_id() {
    return first_id;
  }
  public void set_first_id(Long first_id) {
    this.first_id = first_id;
  }
  public ads with_first_id(Long first_id) {
    this.first_id = first_id;
    return this;
  }
  private Long sources_id;
  public Long get_sources_id() {
    return sources_id;
  }
  public void set_sources_id(Long sources_id) {
    this.sources_id = sources_id;
  }
  public ads with_sources_id(Long sources_id) {
    this.sources_id = sources_id;
    return this;
  }
  private Long incoming_id;
  public Long get_incoming_id() {
    return incoming_id;
  }
  public void set_incoming_id(Long incoming_id) {
    this.incoming_id = incoming_id;
  }
  public ads with_incoming_id(Long incoming_id) {
    this.incoming_id = incoming_id;
    return this;
  }
  private String url;
  public String get_url() {
    return url;
  }
  public void set_url(String url) {
    this.url = url;
  }
  public ads with_url(String url) {
    this.url = url;
    return this;
  }
  private String title;
  public String get_title() {
    return title;
  }
  public void set_title(String title) {
    this.title = title;
  }
  public ads with_title(String title) {
    this.title = title;
    return this;
  }
  private String text;
  public String get_text() {
    return text;
  }
  public void set_text(String text) {
    this.text = text;
  }
  public ads with_text(String text) {
    this.text = text;
    return this;
  }
  private String type;
  public String get_type() {
    return type;
  }
  public void set_type(String type) {
    this.type = type;
  }
  public ads with_type(String type) {
    this.type = type;
    return this;
  }
  private String sid;
  public String get_sid() {
    return sid;
  }
  public void set_sid(String sid) {
    this.sid = sid;
  }
  public ads with_sid(String sid) {
    this.sid = sid;
    return this;
  }
  private String region;
  public String get_region() {
    return region;
  }
  public void set_region(String region) {
    this.region = region;
  }
  public ads with_region(String region) {
    this.region = region;
    return this;
  }
  private String city;
  public String get_city() {
    return city;
  }
  public void set_city(String city) {
    this.city = city;
  }
  public ads with_city(String city) {
    this.city = city;
    return this;
  }
  private String state;
  public String get_state() {
    return state;
  }
  public void set_state(String state) {
    this.state = state;
  }
  public ads with_state(String state) {
    this.state = state;
    return this;
  }
  private String country;
  public String get_country() {
    return country;
  }
  public void set_country(String country) {
    this.country = country;
  }
  public ads with_country(String country) {
    this.country = country;
    return this;
  }
  private String phone;
  public String get_phone() {
    return phone;
  }
  public void set_phone(String phone) {
    this.phone = phone;
  }
  public ads with_phone(String phone) {
    this.phone = phone;
    return this;
  }
  private String age;
  public String get_age() {
    return age;
  }
  public void set_age(String age) {
    this.age = age;
  }
  public ads with_age(String age) {
    this.age = age;
    return this;
  }
  private String website;
  public String get_website() {
    return website;
  }
  public void set_website(String website) {
    this.website = website;
  }
  public ads with_website(String website) {
    this.website = website;
    return this;
  }
  private String email;
  public String get_email() {
    return email;
  }
  public void set_email(String email) {
    this.email = email;
  }
  public ads with_email(String email) {
    this.email = email;
    return this;
  }
  private String gender;
  public String get_gender() {
    return gender;
  }
  public void set_gender(String gender) {
    this.gender = gender;
  }
  public ads with_gender(String gender) {
    this.gender = gender;
    return this;
  }
  private String service;
  public String get_service() {
    return service;
  }
  public void set_service(String service) {
    this.service = service;
  }
  public ads with_service(String service) {
    this.service = service;
    return this;
  }
  private java.sql.Timestamp posttime;
  public java.sql.Timestamp get_posttime() {
    return posttime;
  }
  public void set_posttime(java.sql.Timestamp posttime) {
    this.posttime = posttime;
  }
  public ads with_posttime(java.sql.Timestamp posttime) {
    this.posttime = posttime;
    return this;
  }
  private java.sql.Timestamp importtime;
  public java.sql.Timestamp get_importtime() {
    return importtime;
  }
  public void set_importtime(java.sql.Timestamp importtime) {
    this.importtime = importtime;
  }
  public ads with_importtime(java.sql.Timestamp importtime) {
    this.importtime = importtime;
    return this;
  }
  private java.sql.Timestamp modtime;
  public java.sql.Timestamp get_modtime() {
    return modtime;
  }
  public void set_modtime(java.sql.Timestamp modtime) {
    this.modtime = modtime;
  }
  public ads with_modtime(java.sql.Timestamp modtime) {
    this.modtime = modtime;
    return this;
  }
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ads)) {
      return false;
    }
    ads that = (ads) o;
    boolean equal = true;
    equal = equal && (this.id == null ? that.id == null : this.id.equals(that.id));
    equal = equal && (this.first_id == null ? that.first_id == null : this.first_id.equals(that.first_id));
    equal = equal && (this.sources_id == null ? that.sources_id == null : this.sources_id.equals(that.sources_id));
    equal = equal && (this.incoming_id == null ? that.incoming_id == null : this.incoming_id.equals(that.incoming_id));
    equal = equal && (this.url == null ? that.url == null : this.url.equals(that.url));
    equal = equal && (this.title == null ? that.title == null : this.title.equals(that.title));
    equal = equal && (this.text == null ? that.text == null : this.text.equals(that.text));
    equal = equal && (this.type == null ? that.type == null : this.type.equals(that.type));
    equal = equal && (this.sid == null ? that.sid == null : this.sid.equals(that.sid));
    equal = equal && (this.region == null ? that.region == null : this.region.equals(that.region));
    equal = equal && (this.city == null ? that.city == null : this.city.equals(that.city));
    equal = equal && (this.state == null ? that.state == null : this.state.equals(that.state));
    equal = equal && (this.country == null ? that.country == null : this.country.equals(that.country));
    equal = equal && (this.phone == null ? that.phone == null : this.phone.equals(that.phone));
    equal = equal && (this.age == null ? that.age == null : this.age.equals(that.age));
    equal = equal && (this.website == null ? that.website == null : this.website.equals(that.website));
    equal = equal && (this.email == null ? that.email == null : this.email.equals(that.email));
    equal = equal && (this.gender == null ? that.gender == null : this.gender.equals(that.gender));
    equal = equal && (this.service == null ? that.service == null : this.service.equals(that.service));
    equal = equal && (this.posttime == null ? that.posttime == null : this.posttime.equals(that.posttime));
    equal = equal && (this.importtime == null ? that.importtime == null : this.importtime.equals(that.importtime));
    equal = equal && (this.modtime == null ? that.modtime == null : this.modtime.equals(that.modtime));
    return equal;
  }
  public void readFields(ResultSet __dbResults) throws SQLException {
    this.__cur_result_set = __dbResults;
    this.id = JdbcWritableBridge.readLong(1, __dbResults);
    this.first_id = JdbcWritableBridge.readLong(2, __dbResults);
    this.sources_id = JdbcWritableBridge.readLong(3, __dbResults);
    this.incoming_id = JdbcWritableBridge.readLong(4, __dbResults);
    this.url = JdbcWritableBridge.readString(5, __dbResults);
    this.title = JdbcWritableBridge.readString(6, __dbResults);
    this.text = JdbcWritableBridge.readString(7, __dbResults);
    this.type = JdbcWritableBridge.readString(8, __dbResults);
    this.sid = JdbcWritableBridge.readString(9, __dbResults);
    this.region = JdbcWritableBridge.readString(10, __dbResults);
    this.city = JdbcWritableBridge.readString(11, __dbResults);
    this.state = JdbcWritableBridge.readString(12, __dbResults);
    this.country = JdbcWritableBridge.readString(13, __dbResults);
    this.phone = JdbcWritableBridge.readString(14, __dbResults);
    this.age = JdbcWritableBridge.readString(15, __dbResults);
    this.website = JdbcWritableBridge.readString(16, __dbResults);
    this.email = JdbcWritableBridge.readString(17, __dbResults);
    this.gender = JdbcWritableBridge.readString(18, __dbResults);
    this.service = JdbcWritableBridge.readString(19, __dbResults);
    this.posttime = JdbcWritableBridge.readTimestamp(20, __dbResults);
    this.importtime = JdbcWritableBridge.readTimestamp(21, __dbResults);
    this.modtime = JdbcWritableBridge.readTimestamp(22, __dbResults);
  }
  public void loadLargeObjects(LargeObjectLoader __loader)
      throws SQLException, IOException, InterruptedException {
  }
  public void write(PreparedStatement __dbStmt) throws SQLException {
    write(__dbStmt, 0);
  }

  public int write(PreparedStatement __dbStmt, int __off) throws SQLException {
    JdbcWritableBridge.writeLong(id, 1 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(first_id, 2 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(sources_id, 3 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeLong(incoming_id, 4 + __off, -5, __dbStmt);
    JdbcWritableBridge.writeString(url, 5 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(title, 6 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(text, 7 + __off, -1, __dbStmt);
    JdbcWritableBridge.writeString(type, 8 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(sid, 9 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(region, 10 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(city, 11 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(state, 12 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(country, 13 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(phone, 14 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(age, 15 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(website, 16 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(email, 17 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(gender, 18 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeString(service, 19 + __off, 12, __dbStmt);
    JdbcWritableBridge.writeTimestamp(posttime, 20 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(importtime, 21 + __off, 93, __dbStmt);
    JdbcWritableBridge.writeTimestamp(modtime, 22 + __off, 93, __dbStmt);
    return 22;
  }
  public void readFields(DataInput __dataIn) throws IOException {
    if (__dataIn.readBoolean()) { 
        this.id = null;
    } else {
    this.id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.first_id = null;
    } else {
    this.first_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.sources_id = null;
    } else {
    this.sources_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.incoming_id = null;
    } else {
    this.incoming_id = Long.valueOf(__dataIn.readLong());
    }
    if (__dataIn.readBoolean()) { 
        this.url = null;
    } else {
    this.url = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.title = null;
    } else {
    this.title = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.text = null;
    } else {
    this.text = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.type = null;
    } else {
    this.type = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.sid = null;
    } else {
    this.sid = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.region = null;
    } else {
    this.region = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.city = null;
    } else {
    this.city = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.state = null;
    } else {
    this.state = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.country = null;
    } else {
    this.country = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.phone = null;
    } else {
    this.phone = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.age = null;
    } else {
    this.age = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.website = null;
    } else {
    this.website = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.email = null;
    } else {
    this.email = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.gender = null;
    } else {
    this.gender = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.service = null;
    } else {
    this.service = Text.readString(__dataIn);
    }
    if (__dataIn.readBoolean()) { 
        this.posttime = null;
    } else {
    this.posttime = new Timestamp(__dataIn.readLong());
    this.posttime.setNanos(__dataIn.readInt());
    }
    if (__dataIn.readBoolean()) { 
        this.importtime = null;
    } else {
    this.importtime = new Timestamp(__dataIn.readLong());
    this.importtime.setNanos(__dataIn.readInt());
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
    if (null == this.first_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.first_id);
    }
    if (null == this.sources_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.sources_id);
    }
    if (null == this.incoming_id) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.incoming_id);
    }
    if (null == this.url) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, url);
    }
    if (null == this.title) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, title);
    }
    if (null == this.text) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, text);
    }
    if (null == this.type) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, type);
    }
    if (null == this.sid) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, sid);
    }
    if (null == this.region) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, region);
    }
    if (null == this.city) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, city);
    }
    if (null == this.state) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, state);
    }
    if (null == this.country) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, country);
    }
    if (null == this.phone) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, phone);
    }
    if (null == this.age) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, age);
    }
    if (null == this.website) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, website);
    }
    if (null == this.email) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, email);
    }
    if (null == this.gender) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, gender);
    }
    if (null == this.service) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    Text.writeString(__dataOut, service);
    }
    if (null == this.posttime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.posttime.getTime());
    __dataOut.writeInt(this.posttime.getNanos());
    }
    if (null == this.importtime) { 
        __dataOut.writeBoolean(true);
    } else {
        __dataOut.writeBoolean(false);
    __dataOut.writeLong(this.importtime.getTime());
    __dataOut.writeInt(this.importtime.getNanos());
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
    __sb.append(FieldFormatter.escapeAndEnclose(first_id==null?"null":"" + first_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sources_id==null?"null":"" + sources_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(incoming_id==null?"null":"" + incoming_id, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(url==null?"null":url, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(title==null?"null":title, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(text==null?"null":text, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(type==null?"null":type, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(sid==null?"null":sid, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(region==null?"null":region, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(city==null?"null":city, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(state==null?"null":state, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(country==null?"null":country, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(phone==null?"null":phone, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(age==null?"null":age, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(website==null?"null":website, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(email==null?"null":email, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(gender==null?"null":gender, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(service==null?"null":service, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(posttime==null?"null":"" + posttime, delimiters));
    __sb.append(fieldDelim);
    __sb.append(FieldFormatter.escapeAndEnclose(importtime==null?"null":"" + importtime, delimiters));
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
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.first_id = null; } else {
      this.first_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.sources_id = null; } else {
      this.sources_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.incoming_id = null; } else {
      this.incoming_id = Long.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.url = null; } else {
      this.url = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.title = null; } else {
      this.title = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.text = null; } else {
      this.text = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.type = null; } else {
      this.type = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.sid = null; } else {
      this.sid = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.region = null; } else {
      this.region = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.city = null; } else {
      this.city = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.state = null; } else {
      this.state = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.country = null; } else {
      this.country = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.phone = null; } else {
      this.phone = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.age = null; } else {
      this.age = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.website = null; } else {
      this.website = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.email = null; } else {
      this.email = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.gender = null; } else {
      this.gender = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null")) { this.service = null; } else {
      this.service = __cur_str;
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.posttime = null; } else {
      this.posttime = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.importtime = null; } else {
      this.importtime = java.sql.Timestamp.valueOf(__cur_str);
    }

    __cur_str = __it.next();
    if (__cur_str.equals("null") || __cur_str.length() == 0) { this.modtime = null; } else {
      this.modtime = java.sql.Timestamp.valueOf(__cur_str);
    }

    } catch (RuntimeException e) {    throw new RuntimeException("Can't parse input data: '" + __cur_str + "'", e);    }  }

  public Object clone() throws CloneNotSupportedException {
    ads o = (ads) super.clone();
    o.posttime = (o.posttime != null) ? (java.sql.Timestamp) o.posttime.clone() : null;
    o.importtime = (o.importtime != null) ? (java.sql.Timestamp) o.importtime.clone() : null;
    o.modtime = (o.modtime != null) ? (java.sql.Timestamp) o.modtime.clone() : null;
    return o;
  }

  public Map<String, Object> getFieldMap() {
    Map<String, Object> __sqoop$field_map = new TreeMap<String, Object>();
    __sqoop$field_map.put("id", this.id);
    __sqoop$field_map.put("first_id", this.first_id);
    __sqoop$field_map.put("sources_id", this.sources_id);
    __sqoop$field_map.put("incoming_id", this.incoming_id);
    __sqoop$field_map.put("url", this.url);
    __sqoop$field_map.put("title", this.title);
    __sqoop$field_map.put("text", this.text);
    __sqoop$field_map.put("type", this.type);
    __sqoop$field_map.put("sid", this.sid);
    __sqoop$field_map.put("region", this.region);
    __sqoop$field_map.put("city", this.city);
    __sqoop$field_map.put("state", this.state);
    __sqoop$field_map.put("country", this.country);
    __sqoop$field_map.put("phone", this.phone);
    __sqoop$field_map.put("age", this.age);
    __sqoop$field_map.put("website", this.website);
    __sqoop$field_map.put("email", this.email);
    __sqoop$field_map.put("gender", this.gender);
    __sqoop$field_map.put("service", this.service);
    __sqoop$field_map.put("posttime", this.posttime);
    __sqoop$field_map.put("importtime", this.importtime);
    __sqoop$field_map.put("modtime", this.modtime);
    return __sqoop$field_map;
  }

  public void setField(String __fieldName, Object __fieldVal) {
    if ("id".equals(__fieldName)) {
      this.id = (Long) __fieldVal;
    }
    else    if ("first_id".equals(__fieldName)) {
      this.first_id = (Long) __fieldVal;
    }
    else    if ("sources_id".equals(__fieldName)) {
      this.sources_id = (Long) __fieldVal;
    }
    else    if ("incoming_id".equals(__fieldName)) {
      this.incoming_id = (Long) __fieldVal;
    }
    else    if ("url".equals(__fieldName)) {
      this.url = (String) __fieldVal;
    }
    else    if ("title".equals(__fieldName)) {
      this.title = (String) __fieldVal;
    }
    else    if ("text".equals(__fieldName)) {
      this.text = (String) __fieldVal;
    }
    else    if ("type".equals(__fieldName)) {
      this.type = (String) __fieldVal;
    }
    else    if ("sid".equals(__fieldName)) {
      this.sid = (String) __fieldVal;
    }
    else    if ("region".equals(__fieldName)) {
      this.region = (String) __fieldVal;
    }
    else    if ("city".equals(__fieldName)) {
      this.city = (String) __fieldVal;
    }
    else    if ("state".equals(__fieldName)) {
      this.state = (String) __fieldVal;
    }
    else    if ("country".equals(__fieldName)) {
      this.country = (String) __fieldVal;
    }
    else    if ("phone".equals(__fieldName)) {
      this.phone = (String) __fieldVal;
    }
    else    if ("age".equals(__fieldName)) {
      this.age = (String) __fieldVal;
    }
    else    if ("website".equals(__fieldName)) {
      this.website = (String) __fieldVal;
    }
    else    if ("email".equals(__fieldName)) {
      this.email = (String) __fieldVal;
    }
    else    if ("gender".equals(__fieldName)) {
      this.gender = (String) __fieldVal;
    }
    else    if ("service".equals(__fieldName)) {
      this.service = (String) __fieldVal;
    }
    else    if ("posttime".equals(__fieldName)) {
      this.posttime = (java.sql.Timestamp) __fieldVal;
    }
    else    if ("importtime".equals(__fieldName)) {
      this.importtime = (java.sql.Timestamp) __fieldVal;
    }
    else    if ("modtime".equals(__fieldName)) {
      this.modtime = (java.sql.Timestamp) __fieldVal;
    }
    else {
      throw new RuntimeException("No such field: " + __fieldName);
    }
  }
}
