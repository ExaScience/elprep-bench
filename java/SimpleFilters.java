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
import java.lang.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

public class SimpleFilters {

  private static final Slice SN = new Slice("SN");

  public static Function<SamHeader, Predicate<SamAlignment>> replaceReferenceSequenceDictionary (List<Map<Slice, Slice>> dict) {
    return (header) -> {
      var sortingOrder = header.getHD_SO();
      if (sortingOrder.equals(SamHeader.coordinate)) {
        var previousPos = -1;
        var oldDict = header.SQ;
        for (var entry : dict) {
          var sn = entry.get(SN);
          var pos = SamHeader.find(oldDict, (e) -> sn.equals(e.get(SN)));
          if (pos >= 0) {
            if (pos > previousPos) {
              previousPos = pos;
            } else {
              header.setHD_SO(SamHeader.unknown);
              break;
            }
          }
        }
      }
      var dictTable = new HashSet<Slice>();
      for (var entry : dict) {
        dictTable.add(entry.get(SN));
      }
      header.SQ = dict;
      return (aln) -> dictTable.contains(aln.RNAME);
    };
  }

  public static Function<SamHeader, Predicate<SamAlignment>> replaceReferenceSequenceDictionaryFromSamFile (String samFile) {
    try (var reader = new BufferedReader(new InputStreamReader(new FileInputStream(samFile), "US-ASCII"))) {
      var header = new SamHeader(reader);
      return replaceReferenceSequenceDictionary(header.SQ);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Predicate<SamAlignment> filterUnmappedReads (SamHeader header) {
    return (aln) -> (aln.FLAG & SamAlignment.Unmapped) == 0;
  }

  private static final Slice star = new Slice("*");

  public static Predicate<SamAlignment> filterUnmappedReadsStrict (SamHeader header) {
    return (aln) -> ((aln.FLAG & SamAlignment.Unmapped) == 0) && (aln.POS != 0) && (!aln.RNAME.equals(star));
  }

  public static Predicate<SamAlignment> filterDuplicateReads (SamHeader header) {
    return (aln) -> (aln.FLAG & SamAlignment.Duplicate) == 0;
  }

  private static final Slice atsr = new Slice("@sr");
  private static final Slice sr = new Slice("sr");

  public static Predicate<SamAlignment> filterOptionalReads (SamHeader header) {
    if (header.userRecords.get(atsr) == null) {
      return null;
    } else {
      header.userRecords.remove(atsr);
      return (aln) -> Fields.assoc(aln.TAGS, sr) == null;
    }
  }

  private static final Slice ID = new Slice("ID");

  public static Function<SamHeader, Predicate<SamAlignment>> addOrReplaceReadGroup (Map<Slice, Slice> readGroup) {
    return (header) -> {
      header.RG = new ArrayList<>();
      header.RG.add(readGroup);
      var id = readGroup.get(ID);
      return (aln) -> {aln.setRG(id); return true;};
    };
  }

  private static final Slice PP = new Slice("PP");

  public static Function<SamHeader, Predicate<SamAlignment>> addPGLine (Map<Slice, Slice> newPG) {
    return (header) -> {
      String[] id = new String[]{newPG.get(ID).toString()};
      while (SamHeader.find(header.PG, (entry) -> id[0].equals(entry.get(ID))) >= 0) {
        id[0] += Integer.toHexString(ThreadLocalRandom.current().nextInt(0x10000));
      }
      newPG.put(ID, new Slice(id[0]));
      for (var pg : header.PG) {
        var nextId = pg.get(ID);
        var pos = SamHeader.find(header.PG, (entry) -> nextId.equals(entry.get(PP)));
        if (pos < 0) {
          newPG.put(PP, nextId); break;
        }
      }
      header.PG.add(newPG);
      return null;
    };
  }

  private static final Slice eq = new Slice("=");

  public static Predicate<SamAlignment> renameChromosomes (SamHeader header) {
    for (var entry : header.SQ) {
      var sn = entry.get(SN);
      if (sn != null) {
        entry.put(SN, new Slice("chr" + sn.toString()));
      }
    }
    return (aln) -> {
      if ((!aln.RNAME.equals(eq)) && (!aln.RNAME.equals(star))) {
        aln.RNAME = new Slice("chr" + aln.RNAME.toString());
      }
      if ((!aln.RNEXT.equals(eq)) && (!aln.RNEXT.equals(star))) {
        aln.RNEXT = new Slice("chr" + aln.RNEXT.toString());
      }
      return true;
    };
  }

  public static Predicate<SamAlignment> addREFID (SamHeader header) {
    var dictTable = new HashMap<Slice, Integer>();
    var index = -1;
    for (var entry : header.SQ) {
      ++index;
      dictTable.put(entry.get(SN), index);
    }
    return (aln) -> {
      var value = dictTable.get(aln.RNAME);
      var val = (value == null ? -1 : value);
      aln.setREFID(val);
      return true;
    };
  }
}
