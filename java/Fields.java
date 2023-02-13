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

public class Fields {

  public static class Field {
    public final Slice tag;

    public Field (Slice tag) {
      this.tag = tag;
    }

    public void format (PrintWriter out) {
      out.print('\t');
      tag.write(out);
    }
  }

  public static Field assoc (List<Field> l, Slice tag) {
    for (var f : l) {
      if (f.tag.equals(tag)) {
        return f;
      }
    }
    return null;
  }

  public static class CharacterField extends Field {
    public char value;

    public CharacterField (Slice tag, char value) {
      super(tag);
      this.value = value;
    }

    @Override public void format (PrintWriter out) {
      super.format(out);
      out.print(":A:");
      out.print(value);
    }
  }

  public static class StringField extends Field {
    public Slice value;

    public StringField (Slice tag, Slice value) {
      super(tag);
      this.value = value;
    }

    @Override public void format (PrintWriter out) {
      super.format(out);
      out.print(":Z:");
      value.write(out);
    }
  }

  public static class IntegerField extends Field {
    public int value;

    public IntegerField (Slice tag, int value) {
      super(tag);
      this.value = value;
    }

    @Override public void format (PrintWriter out) {
      super.format(out);
      out.print(":i:");
      out.print(value);
    }
  }

  public static class FloatField extends Field {
    public float value;

    public FloatField (Slice tag, float value) {
      super(tag);
      this.value = value;
    }

    @Override public void format (PrintWriter out) {
      super.format(out);
      out.print(":f:");
      out.print(value);
    }
  }

  public static class ByteArrayField extends Field {
    public short[] value;

    public ByteArrayField (Slice tag, short[] value) {
      super(tag);
      this.value = value;
    }

    @Override public void format (PrintWriter out) {
      super.format(out);
      out.print(":H:");
      for (var val : value) {
        if (val < 16) {
            out.print('0');
        }
        out.print(Integer.toHexString(val));
      }
    }
  }

  public static class NumericArrayField<T> extends Field {
    char ntype;
    public List<T> value;

    public NumericArrayField (Slice tag, char ntype, List<T> value) {
      super(tag);
      this.ntype = ntype;
      this.value = value;
    }

    @Override public void format (PrintWriter out) {
      super.format(out);
      out.print(":B:");
      out.print(ntype);
      for (var val : value) {
        out.print(',');
        out.print(val);
      }
    }
  }
}
