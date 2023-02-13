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
