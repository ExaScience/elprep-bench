/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

package elprep;

import java.lang.*;
import java.util.*;
import java.util.concurrent.*;

public class CigarOperation {
  public static final String CigarOperations = "MmIiDdNnSsHhPpXx=";

  private static Map<Character, Character> cigarOperationsTable = new HashMap<>();

  static {
    for (var i = 0; i < CigarOperations.length(); ++i) {
      var c = CigarOperations.charAt(i);
      cigarOperationsTable.put(c, Character.toUpperCase(c));
    }
  }

  public final int length;
  public final char operation;

  public CigarOperation (int length, char operation) {
    this.length = length;
    this.operation = operation;
  }

  public static boolean isDigit (char c) {
    return (('0' <= c) && (c <= '9'));
  }

  static class cigarScanner {
    int index = 0;
    final Slice cigar;

    cigarScanner (Slice cigar) {
      this.cigar = cigar;
    }

    CigarOperation makeCigarOperation () {
      for (var j = index;; ++j) {
        var c = cigar.charAt(j);
        if (!isDigit(c)) {
          var length = Integer.parseUnsignedInt(cigar, index, j, 10);
          var operation = cigarOperationsTable.get(c);
          index = j+1;
          return new CigarOperation(length, operation);
        }
      }
    }
  }

  private static Map<Slice, List<CigarOperation>> cigarListCache = new ConcurrentHashMap<>();

  static {
    cigarListCache.put(new Slice("*"), new ArrayList<>());
  }

  private static List<CigarOperation> slowScanCigarString (Slice cigar) {
    var list = new ArrayList<CigarOperation>();
    var scanner = new cigarScanner(cigar);
    while (scanner.index < cigar.length()) {
      list.add(scanner.makeCigarOperation());
    }
    var old = cigarListCache.putIfAbsent(cigar, list);
    if (old == null) {
      return list;
    } else {
      return old;
    }
  }

  public static List<CigarOperation> scanCigarString (Slice cigar) {
    var list = cigarListCache.get(cigar);
    if (list == null) {
      return slowScanCigarString(cigar);
    } else {
      return list;
    }
  }
}
