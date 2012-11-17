//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections;
using System.Security.Cryptography;

namespace Deveel.Data.Util {
	internal class CollectionsUtil {
		public static ICollection Shuffle(ICollection c) {
			if (c == null || c.Count <= 1) {
				return c;
			}

			byte[] bytes = new byte[4];
			RNGCryptoServiceProvider cRandom = new RNGCryptoServiceProvider();
			cRandom.GetBytes(bytes);

			int seed = BitConverter.ToInt32(bytes, 0);
			Random random = new Random(seed);

			ArrayList orig = new ArrayList(c);
			ArrayList randomized = new ArrayList(c.Count);
			for (int i = 0; i < c.Count; i++) {
				int index = random.Next(orig.Count);
				randomized.Add(orig[index]);
				orig.RemoveAt(index);
			}
			return randomized;
		}

		public static void Swap(IList list, int i, int i1) {
			object value1 = list[i];
			object value2 = list[i1];
			list[i] = value2;
			list[i1] = value1;
		}
	}
}