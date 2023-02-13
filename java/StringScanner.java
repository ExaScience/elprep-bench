// elprep-bench.
// Copyright (c) 2018-2023 imec vzw.

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version, and Additional Terms
// (see below).

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License and Additional Terms along with this program. If not, see
// <https://github.com/ExaScience/elprep-bench/blob/master/LICENSE.txt>.

package elprep;

import java.lang.*;
import java.util.*;

import static elprep.Fields.*;

public class StringScanner {
  private String data;
  private int pos;
  private int length;

  private char charResult;
  private Slice sliceResult;

  public StringScanner () {}

  public StringScanner (String s) {
    this(s, 0, s.length());
  }

  public StringScanner (String s, int pos, int length) {
    this.data = s;
    this.pos = pos;
    this.length = length;
  }

  public void reset (String s) {
    reset(s, 0, s.length());
  }

  public void reset (String s, int pos, int length) {
    this.data = s;
    this.pos = pos;
    this.length = length;
  }

  public int length () {
    return length - pos;
  }

  public boolean readCharUntil (char c) {
    var start = pos;
    var next = pos+1;
    if (next >= length) {
      pos = length;
      charResult = data.charAt(start);
      return false;
    } else if (data.charAt(next) != c) {
      throw new RuntimeException("Unexpected character " + data.charAt(next) + " in StringScanner.readCharUntil.");
    } else {
      pos = next+1;
      charResult = data.charAt(start);
      return true;
    }
  }

  public boolean readUntil (char c) {
    var start = pos;
    for (var end = pos; end < length; ++end) {
      if (data.charAt(end) == c) {
        pos = end+1;
        sliceResult = new Slice(data, start, end-start);
        return true;
      }
    }
    pos = length;
    sliceResult = new Slice(data, start, length-start);
    return false;
  }

  public char readUntil2 (char c1, char c2) {
    var start = pos;
    for (var end = pos; end < length; ++end) {
      var c = data.charAt(end);
      if ((c == c1) || (c == c2)) {
        pos = end+1;
        sliceResult = new Slice(data, start, end-start);
        return c;
      }
    }
    pos = length;
    sliceResult = new Slice(data, start, length-start);
    return 0;
  }

  public Map<Slice, Slice> parseSamHeaderLine () {
    var record = new HashMap<Slice, Slice>(8);
    while (length() > 0) {
      var ok = readUntil(':');
      var tag = sliceResult;
      assert ok && (tag.length() == 2) : "Invalid field type tag " + tag + ".";
      readUntil('\t');
      var value = sliceResult;
      if (!Slice.setUniqueEntry(record, tag, value)) {
        throw new RuntimeException("Duplicate field tag " + tag + " in SAM header line.");
      }
    }
    return record;
  }

  public static Map<Slice, Slice> parseSamHeaderLineFromString (String line) {
    var record = new HashMap<Slice, Slice>(8);
    var fields = line.split("\\s+");
    for (var field : fields) {
      assert field.charAt(2) == ':' : "Incorrectly formatted SAM file field: " + field + " .";
      var tag = new Slice(field.substring(0,2));
      var value = new Slice(field.substring(3));
      if (!Slice.setUniqueEntry(record, tag, value)) {
        throw new RuntimeException("Duplicate field tag " + tag + " in SAM header line.");
      }
    }
    return record;
  }

  public static interface FieldParser {
    Field parse (StringScanner sc, Slice tag);
  }

  public Field parseSamChar (Slice tag) {
    readCharUntil('\t'); return new CharacterField(tag, charResult);
  }

  public Field parseSamInteger (Slice tag) {
    readUntil('\t'); return new IntegerField(tag, Integer.parseInt(sliceResult, 0, sliceResult.length(), 10));
  }

  public Field parseSamFloat (Slice tag) {
    readUntil('\t'); return new FloatField(tag, Float.parseFloat(sliceResult.toString()));
  }

  public Field parseSamString (Slice tag) {
    readUntil('\t'); return new StringField(tag, sliceResult);
  }

  public Field parseSamByteArray (Slice tag) {
    readUntil('\t'); var value = sliceResult;
    var byteArray = new short[value.length >> 1];
    for (var i = 0; i < value.length; i += 2) {
      byteArray[i >> 1] = (short)Integer.parseUnsignedInt(sliceResult, i, i+2, 16);
    }
    return new ByteArrayField(tag, byteArray);
  }

  public Field parseSamNumericArray (Slice tag) {
    var ok = readUntil(',');
    assert ok : "Missing entry in numeric array.";
    var ntype = charResult;
    char sep;
    switch (ntype) {
    case 'c': {
      var nums = new ArrayList<Byte>();
      do {
        sep = readUntil2(',', '\t');
        nums.add((byte)Integer.parseInt(sliceResult, 0, sliceResult.length(), 10));
      } while (sep == ',');
      return new NumericArrayField<Byte>(tag, ntype, nums);
    }
    case 'C': {
      var nums = new ArrayList<Short>();
      do {
        sep = readUntil2(',', '\t');
        nums.add((short)Integer.parseUnsignedInt(sliceResult, 0, sliceResult.length(), 10));
      } while (sep == ',');
      return new NumericArrayField<Short>(tag, ntype, nums);
    }
    case 's': {
      var nums = new ArrayList<Short>();
      do {
        sep = readUntil2(',', '\t');
        nums.add((short)Integer.parseInt(sliceResult, 0, sliceResult.length(), 10));
      } while (sep == ',');
      return new NumericArrayField<Short>(tag, ntype, nums);
    }
    case 'S': {
      var nums = new ArrayList<Integer>();
      do {
        sep = readUntil2(',', '\t');
        nums.add(Integer.parseUnsignedInt(sliceResult, 0, sliceResult.length(), 10));
      } while (sep == ',');
      return new NumericArrayField<Integer>(tag, ntype, nums);
    }
    case 'i': {
      var nums = new ArrayList<Integer>();
      do {
        sep = readUntil2(',', '\t');
        nums.add(Integer.parseInt(sliceResult, 0, sliceResult.length(), 10));
      } while (sep == ',');
      return new NumericArrayField<Integer>(tag, ntype, nums);
    }
    case 'I': {
      var nums = new ArrayList<Long>();
      do {
        sep = readUntil2(',', '\t');
        nums.add(Long.parseUnsignedLong(sliceResult, 0, sliceResult.length(), 10));
      } while (sep == ',');
      return new NumericArrayField<Long>(tag, ntype, nums);
    }
    case 'f': {
      var nums = new ArrayList<Float>();
      do {
        sep = readUntil2(',', '\t');
        nums.add(Float.parseFloat(sliceResult.toString()));
      } while (sep == ',');
      return new NumericArrayField<Float>(tag, ntype, nums);
    }
    default:
      throw new RuntimeException("Invalid numeric array type " + ntype + ".");
    }
  }

  public static FieldParser[] optionalFieldParseTable = new FieldParser[41];

  static {
    optionalFieldParseTable['A'-'A'] = StringScanner::parseSamChar;
    optionalFieldParseTable['i'-'A'] = StringScanner::parseSamInteger;
    optionalFieldParseTable['f'-'A'] = StringScanner::parseSamFloat;
    optionalFieldParseTable['Z'-'A'] = StringScanner::parseSamString;
    optionalFieldParseTable['H'-'A'] = StringScanner::parseSamByteArray;
    optionalFieldParseTable['B'-'A'] = StringScanner::parseSamNumericArray;
  }

  public Slice doSlice () {
    var ok = readUntil('\t');
    assert ok : "Missing tabulator in SAM alignment line.";
    return sliceResult;
  }

  public Slice doSlicen () {
    readUntil('\t'); return sliceResult;
  }

  public int doInt () {
    doSlice(); return Integer.parseInt(sliceResult, 0, sliceResult.length(), 10);
  }

  public Field parseSamAlignmentField () {
    var ok = readUntil(':');
    var tag = sliceResult;
    assert ok && tag.length == 2 : "Invalid field tag " + tag + " in SAM alignment line.";
    ok = readCharUntil(':');
    var typebyte = charResult;
    assert ok : "Invalid field type " + typebyte + " in SAM alignment line.";
    return optionalFieldParseTable[typebyte-'A'].parse(this, tag);
  }
}
