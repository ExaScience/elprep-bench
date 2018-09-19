/*
	Copyright (c) 2018 by imec vzw, Leuven, Belgium. All rights reserverd.
*/

package elprep;

import java.io.*;
import java.lang.*;
import java.util.*;
import java.util.concurrent.*;

public final class Slice implements CharSequence {
  private final String storage;
  private final int pos;
  public final int length;

  public Slice (String s) {
    this(s, 0, s.length());
  }

  public Slice (String s, int pos, int length) {
    if ((s != null) && (length > 0)) {
      this.storage = s;
      this.pos = pos;
      this.length = length;
    } else {
      this.storage = null;
      this.pos = 0;
      this.length = 0;
    }
  }

  public Slice (Slice slice, int pos, int length) {
    this(slice.storage, slice.pos+pos, length);
  }

  public char charAt(int index) {
    return storage.charAt(pos+index);
  }

  public int length() {
    return length;
  }

  public CharSequence subSequence(int start, int end) {
    return new Slice(this, start, end-start);
  }

  public String toString() {
    return storage.substring(pos, pos+length);
  }

  public int compareTo (Slice that) {
    if (this == that) {
      return 0;
    } else if ((this.storage == that.storage) &&
               (this.pos == that.pos) &&
               (this.length == that.length)) {
      return 0;
    } else {
      var minLen = Math.min(this.length, that.length);
      var j = that.pos;
      for (var i = this.pos; i < this.pos+minLen; ++i, ++j) {
        var thisChar = this.storage.charAt(i);
        var thatChar = that.storage.charAt(j);
        if (thisChar != thatChar) {
          return thisChar - thatChar;
        }
      }
      return this.length - that.length;
    }
  }

  public boolean equals (Object obj) {
    if (this == obj) {
      return true;
    } else if (obj instanceof Slice) {
      var that = (Slice)obj;
      if ((this.storage == that.storage) &&
          (this.pos == that.pos) &&
          (this.length == that.length)) {
        return true;
      } else if (this.length == that.length) {
        var j = that.pos;
        for (var i = this.pos; i < this.pos+this.length; ++i, ++j) {
          if (this.storage.charAt(i) != that.storage.charAt(j)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }
  
  public int hashCode () {
    var result = 13;
    for (var i = pos; i < pos+length; ++i) {
      result = 29 * result + storage.charAt(i);
    }
    return result;
  }

  private static ConcurrentMap<Slice, Slice> interned = new ConcurrentHashMap<>();

  public Slice intern () {
    var prev = interned.putIfAbsent(this, this);
    if (prev == null) {
      return this;
    } else {
      return prev;
    }
  }

  public void write (PrintWriter out) {
    out.write(storage, pos, length);
  }

  public static boolean setUniqueEntry (Map<Slice, Slice> record, Slice key, Slice value) {
    if (record.get(key) == null) {
      record.put(key, value); return true;
    } else {
      return false;
    }
  }
}
