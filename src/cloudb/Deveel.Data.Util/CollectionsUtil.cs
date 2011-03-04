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