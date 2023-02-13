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

import static elprep.Fields.*;

public class SamAlignment {
  public Slice QNAME;
  public char FLAG;
  public Slice RNAME;
  public int POS;
  public byte MAPQ;
  public Slice CIGAR;
  public Slice RNEXT;
  public int PNEXT;
  public int TLEN;
  public Slice SEQ;
  public Slice QUAL;
  public List<Field> TAGS = new ArrayList<>(16);
  public List<Field> temps = new ArrayList<>(4);

  public SamAlignment (String s) {
    var sc = new StringScanner(s);

    QNAME = sc.doSlice();
    FLAG = (char)sc.doInt();
    RNAME = sc.doSlice();
    POS = sc.doInt();
    MAPQ = (byte)sc.doInt();
    CIGAR = sc.doSlice();
    RNEXT = sc.doSlice();
    PNEXT = sc.doInt();
    TLEN = sc.doInt();
    SEQ = sc.doSlice();
    QUAL = sc.doSlicen();

    while (sc.length() > 0) {
      TAGS.add(sc.parseSamAlignmentField());
    }
  }

  public static final Slice RG = new Slice("RG");
  public static final Slice REFID = new Slice("REFID");
  public static final Slice LIBID = new Slice("LIBID");

  public Slice getRG () {
    return ((StringField)assoc(TAGS, RG)).value;
  }

  public void setRG (Slice rg) {
    var f = assoc(TAGS, RG);
    if (f == null) {
      TAGS.add(new StringField(RG, rg));
    } else {
      ((StringField)f).value = rg;
    }
  }

  public int getREFID () {
    return ((IntegerField)assoc(temps, REFID)).value;
  }

  public void setREFID (int refid) {
    var f = assoc(temps, REFID);
    if (f == null) {
      temps.add(new IntegerField(REFID, refid));
    } else {
      ((IntegerField)f).value = refid;
    }
  }

  public Slice getLIBID () {
    return ((StringField)assoc(temps, LIBID)).value;
  }

  public void setLIBID (Slice libid) {
    var f = assoc(temps, LIBID);
    if (f == null) {
      temps.add(new StringField(LIBID, libid));
    } else {
      ((StringField)f).value = libid;
    }
  }

  public static class CoordinateComparator implements Comparator<SamAlignment> {
    public int compare(SamAlignment aln1, SamAlignment aln2) {
      var refid1 = aln1.getREFID();
      var refid2 = aln2.getREFID();
      if (refid1 < refid2) {
        if (refid1 >= 0) {
          return -1;
        } else {
          return +1;
        }
      } else if (refid2 < refid1) {
        if (refid2 < 0) {
          return -1;
        } else {
          return +1;
        }
      } else {
        return aln1.POS - aln2.POS;
      }
    }
  }

  public static final int Multiple      =   0x1;
  public static final int Proper        =   0x2;
  public static final int Unmapped      =   0x4;
  public static final int NextUnmapped  =   0x8;
  public static final int Reversed      =  0x10;
  public static final int NextReversed  =  0x20;
  public static final int First         =  0x40;
  public static final int Last          =  0x80;
  public static final int Secondary     = 0x100;
  public static final int QCFailed      = 0x200;
  public static final int Duplicate     = 0x400;
  public static final int Supplementary = 0x800;

  public boolean isMultiple ()      {return (FLAG & Multiple) != 0;}
  public boolean isProper ()        {return (FLAG & Proper) != 0;}
  public boolean isUnmapped ()      {return (FLAG & Unmapped) != 0;}
  public boolean isNextUnmapped ()  {return (FLAG & NextUnmapped) != 0;}
  public boolean isReversed ()      {return (FLAG & Reversed) != 0;}
  public boolean isNextReversed ()  {return (FLAG & NextReversed) != 0;}
  public boolean isFirst ()         {return (FLAG & First) != 0;}
  public boolean isLast ()          {return (FLAG & Last) != 0;}
  public boolean isSecondary ()     {return (FLAG & Secondary) != 0;}
  public boolean isQCFailed ()      {return (FLAG & QCFailed) != 0;}
  public boolean isDuplicate ()     {return (FLAG & Duplicate) != 0;}
  public boolean isSupplementary () {return (FLAG & Supplementary) != 0;}

  public boolean flagEvery    (int flag) {return (FLAG & flag) == flag;}
  public boolean flagSome     (int flag) {return (FLAG & flag) != 0;}
  public boolean flagNotEvery (int flag) {return (FLAG & flag) != flag;}
  public boolean flagNotAny   (int flag) {return (FLAG & flag) == 0;}

  public void format (PrintWriter out) {
    QNAME.write(out); out.print('\t');
    out.print((int)FLAG); out.print('\t');
    RNAME.write(out); out.print('\t');
    out.print(POS); out.print('\t');
    out.print(MAPQ); out.print('\t');
    CIGAR.write(out); out.print('\t');
    RNEXT.write(out); out.print('\t');
    out.print(PNEXT); out.print('\t');
    out.print(TLEN); out.print('\t');
    SEQ.write(out); out.print('\t');
    QUAL.write(out);

    for (var f : TAGS) {f.format(out);}
  }

  public static short[] phredScoreTable = new short[512];

  static {
    for (var c = 0; c < 256; ++c) {
      var pos = c << 1;
      if ((c < 33) || (c > 126)) {
        phredScoreTable[pos] = 0;
        phredScoreTable[pos+1] = 1;
      } else {
        var qual = c - 33;
        if (qual >= 15) {
          phredScoreTable[pos] = (short)qual;
        } else {
          phredScoreTable[pos] = 0;
        }
        phredScoreTable[pos+1] = 0;
      }
    }
  }

  public int computePhredScore () {
    var score = 0;
    var error = 0;
    for (var i = 0; i < QUAL.length; ++i) {
      var c = QUAL.charAt(i);
      var pos = c << 1;
      score += phredScoreTable[pos];
      error |= phredScoreTable[pos+1];
    }
    assert error != 0 : "Invalid QUAL character in " + QUAL + ".";
    return score;
  }

  public static int[] clippedTable = new int[256];
  public static int[] referenceTable = new int[256];

  static {
    for (var i = 0; i < 256; ++i) {
      clippedTable[i] = 0;
      referenceTable[i] = 0;
    }
    clippedTable['S'] = 1;
    clippedTable['H'] = 1;
    referenceTable['M'] = 1;
    referenceTable['D'] = 1;
    referenceTable['N'] = 1;
    referenceTable['='] = 1;
    referenceTable['X'] = 1;
  }

  public int computeUnclippedPosition () {
    var cigar = CigarOperation.scanCigarString(CIGAR);
    if (cigar.isEmpty()) {return POS;}

    if (isReversed()) {
      var clipped = 1;
      var result = POS-1;
      for (var i = cigar.size()-1; i >= 0; --i) {
        var op = cigar.get(i);
        var p = op.operation;
        var c = clippedTable[p];
        var r = referenceTable[p];
        clipped *= c;
        result += (r|clipped)*op.length;
      }
      return result;
    } else {
      var result = POS;
      for (var op : cigar) {
        var p = op.operation;
        if (clippedTable[p] == 0) {break;}
        result -= op.length;
      }
      return result;
    }
  }

  private static final Slice pos = new Slice("pos");
  private static final Slice score = new Slice("score");


  public int getAdaptedPos () {
    return ((IntegerField)assoc(temps, pos)).value;
  }

  public void setAdaptedPos (int p) {
    var f = assoc(temps, pos);
    if (f == null) {
      temps.add(new IntegerField(pos, p));
    } else {
      ((IntegerField)f).value = p;
    }
  }

  public int getAdaptedScore () {
    return ((IntegerField)assoc(temps, score)).value;
  }

  public void setAdaptedScore (int s) {
    var f = assoc(temps, score);
    if (f == null) {
      temps.add(new IntegerField(score, s));
    } else {
      ((IntegerField)f).value = s;
    }
  }

  public void adaptAlignment (Map<Slice, Slice> lbTable) {
    var rg = getRG();
    if (rg != null) {
      rg = rg.intern();
      setRG(rg);
      var lb = lbTable.get(rg);
      if (lb != null) {
        setLIBID(lb);
      }
    }
    setAdaptedPos(computeUnclippedPosition());
    setAdaptedScore(computePhredScore());
  }

  public boolean isTrueFragment () {
    return (FLAG & (Multiple | NextUnmapped)) != Multiple;
  }

  public boolean isTruePair () {
    return (FLAG & (Multiple | NextUnmapped)) == Multiple;
  }
}
