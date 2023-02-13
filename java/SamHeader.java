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

import java.io.*;
import java.util.*;
import java.util.function.*;

public class SamHeader {

  public static final Slice SamFileFormatVersion = new Slice("1.5");
  public static final Slice SamFileFormatDate = new Slice("1 Jun 2017");

  public Map<Slice, Slice> HD = null;
  public List<Map<Slice, Slice>> SQ = new ArrayList<>(32);
  public List<Map<Slice, Slice>> RG = new ArrayList<>();
  public List<Map<Slice, Slice>> PG = new ArrayList<>(2);
  public List<Slice> CO = new ArrayList<>();
  public Map<Slice, List<Map<Slice, Slice>>> userRecords = new HashMap<>();

  public static boolean isSamHeaderUserTag(String code, int pos, int length) {
    var end = pos+length;
    for (var i = 0; i < end; ++i) {
      var c = code.charAt(i);
      if (('a' <= c) && (c <= 'z')) {
        return true;
      }
    }
    return false;
  }

  public static int find (List<Map<Slice, Slice>> l, Predicate<Map<Slice, Slice>> p) {
    var index = -1;
    for (var e : l) {
      ++index;
      if (p.test(e)) {return index;}
    }
    return -1;
  }

  public SamHeader (BufferedReader reader) {
    try {
      var sc = new StringScanner();
      for (var first = true;; first = false) {
        reader.mark(2);
        var peek = reader.read();
        reader.reset();
        if ((peek < 0)  || (peek != '@')) {
          return;
        } else {
          var line = reader.readLine();
          sc.reset(line, 4, line.length()-4);
          if (line.startsWith("@HD\t")) {
            assert first : "@HD line not in first line when parsing a SAM header.";
            HD = sc.parseSamHeaderLine();
          } else if (line.startsWith("@SQ\t")) {
            SQ.add(sc.parseSamHeaderLine());
          } else if (line.startsWith("@RG\t")) {
            RG.add(sc.parseSamHeaderLine());
          } else if (line.startsWith("@PG\t")) {
            PG.add(sc.parseSamHeaderLine());
          } else if (line.startsWith("@CO\t")) {
            CO.add(new Slice(line, 4, line.length()-4));
          } else if (line.startsWith("@CO")) {
            CO.add(new Slice(line, 3, line.length()-3));
          } else if (isSamHeaderUserTag(line, 1, 2)) {
            assert line.charAt(0) == '@' : "Header code " + line.substring(0, 3) + " does not start with @ when parsing a SAM header.";
            assert line.charAt(3) == '\t' : "Header code " + line.substring(0, 3) + " not followed by a tab when parsing a SAM header.";
            addUserRecord(new Slice(line, 0, 3), sc.parseSamHeaderLine());
          } else {
            throw new RuntimeException("Unknown SAM record type code " + line.substring(0, 3));
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void skipSamHeader (BufferedReader reader) {
    try {
      while (true) {
        reader.mark(1024);
        var line = reader.readLine();
        if ((line == null) || (line.charAt(0) != '@')) {
          reader.reset();
          return;
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static final Slice LN = new Slice("LN");
  public static final Slice VN = new Slice("VN");
  public static final Slice SO = new Slice("SO");
  public static final Slice GO = new Slice("GO");

  public static final Slice coordinate = new Slice("coordinate");
  public static final Slice queryname  = new Slice("queryname");
  public static final Slice unsorted   = new Slice("unsorted");
  public static final Slice unknown    = new Slice("unknown");
  public static final Slice keep       = new Slice("keep");

  public static final Slice none      = new Slice("none");
  public static final Slice query     = new Slice("query");
  public static final Slice reference = new Slice("reference");

  public static int getSQ_LN (Map<Slice, Slice> record) {
    var ln = record.get(LN);
    assert ln != null : "LN entry in a SQ header line missing.";
    return Integer.parseInt(ln, 0, ln.length(), 10);
  }

  public static int getSQ_LN (Map<Slice, Slice> record, int defaultValue) {
    var ln = record.get(LN);
    if (ln == null) {
      return defaultValue;
    } else {
      return Integer.parseInt(ln, 0, ln.length(), 10);
    }
  }

  public static void setSQ_LN (Map<Slice, Slice> record, int value) {
    record.put(LN, new Slice(Integer.toString(value)));
  }

  public Map<Slice, Slice> ensureHD () {
    if (HD == null) {
      HD = new HashMap<>();
      HD.put(VN, SamFileFormatVersion);
    }
    return HD;
  }

  public Slice getHD_SO () {
    var hd = ensureHD();
    var sortingOrder = hd.get(SO);
    if (sortingOrder == null) {
      return unknown;
    } else {
      return sortingOrder;
    }
  }

  public void setHD_SO (Slice value) {
    var hd = ensureHD();
    hd.remove(GO);
    hd.put(SO, value);
  }

  public Slice getHD_GO () {
    var hd = ensureHD();
    var groupingOrder = hd.get(GO);
    if (groupingOrder == null) {
      return none;
    } else {
      return groupingOrder;
    }
  }

  public void setHD_GO (Slice value) {
    var hd = ensureHD();
    hd.remove(SO);
    hd.put(GO, value);
  }

  public void addUserRecord (Slice code, Map<Slice, Slice> record) {
    var records = userRecords.get(code);
    if (records == null) {
      records = new ArrayList<>();
      userRecords.put(code, records);
    }
    records.add(record);
  }

  public static void formatSamString (PrintWriter out, Slice tag, Slice value) {
    out.print('\t');
    tag.write(out);
    out.print(':');
    value.write(out);
  }

  public static void formatSamHeaderLine (PrintWriter out, String code, Map<Slice, Slice> record) {
    out.print(code);
    record.forEach((key, value) -> formatSamString(out, key, value));
    out.print('\n');
  }

  public static void formatSamHeaderLine (PrintWriter out, Slice code, Map<Slice, Slice> record) {
    code.write(out);
    record.forEach((key, value) -> formatSamString(out, key, value));
    out.print('\n');
  }

  public static void formatSamComment (PrintWriter out, String code, Slice comment) {
    out.print(code);
    out.print('\t');
    comment.write(out);
    out.print('\n');
  }

  public static void formatSamComment (PrintWriter out, Slice code, Slice comment) {
    code.write(out);
    out.print('\t');
    comment.write(out);
    out.print('\n');
  }

  private static Slice atHD = new Slice("@HD");
  private static Slice atSQ = new Slice("@SQ");
  private static Slice atRG = new Slice("@RG");
  private static Slice atPG = new Slice("@PG");
  private static Slice atCO = new Slice("@CO");

  public void format (PrintWriter out) {
    if (HD != null) {formatSamHeaderLine(out, "@HD", HD);}
    for (var record : SQ) {formatSamHeaderLine(out, "@SQ", record);}
    for (var record : RG) {formatSamHeaderLine(out, "@RG", record);}
    for (var record : PG) {formatSamHeaderLine(out, "@PG", record);}
    for (var comment : CO) {formatSamComment(out, "@CO", comment);}
    userRecords.forEach((code, records) -> {
        for (var record : records) {
          formatSamHeaderLine(out, code, record);
        }
      });
  }
}
