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

import java.util.*;
import java.util.function.*;

public class Pipeline {
  public static Predicate<SamAlignment> composeFilters (SamHeader header, List<Function<SamHeader, Predicate<SamAlignment>>> filters) {
    if (filters == null) {
      return null;
    } else {
      var alnFilters = new ArrayList<Predicate<SamAlignment>>();
      for (var f : filters) {
        var alnFilter = f.apply(header);
        if (alnFilter != null) {
          alnFilters.add(alnFilter);
        }
      }
      switch (alnFilters.size()) {
      case 0:
        return null;
      case 1:
        return alnFilters.get(0);
      default:
        return (SamAlignment aln) -> {
          for (var p : alnFilters) {
            if (!p.test(aln)) {
              return false;
            }
          }
          return true;
        };
      }
    }
  }

  public static Slice effectiveSortingOrder (Slice sortingOrder, SamHeader header, Slice originalSortingOrder) {
    var so = sortingOrder.equals(SamHeader.keep) ? originalSortingOrder : sortingOrder;
    var currentSortingOrder = header.getHD_SO();
    if (so.equals(SamHeader.coordinate) || so.equals(SamHeader.queryname)) {
      if (currentSortingOrder.equals(so)) {
        return SamHeader.keep;
      }
      header.setHD_SO(so);
    } else if (so.equals(SamHeader.unknown) || so.equals(SamHeader.unsorted)) {
      if (!currentSortingOrder.equals(so)) {
        header.setHD_SO(so);
      }
    }
    return so;
  }
}
