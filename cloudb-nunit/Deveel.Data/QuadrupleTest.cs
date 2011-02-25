using System;

using NUnit.Framework;

namespace Deveel.Data {
	[TestFixture]
	public class QuadrupleTest {
		private static void Spec(int x, int y) {
			Console.Out.WriteLine("spec(" + x + ", " + y + ")");
			if (x != y) {
				throw new ApplicationException("Test Failed");
			}
		}

		[Test]
		public void SpectTest () {
			Spec(new Quadruple(new long[] { 0, 0 }).CompareTo(
	 new Quadruple(new long[] { 0, 0 })), 0);

			Spec(new Quadruple(new long[] { 0, 1 }).CompareTo(
				 new Quadruple(new long[] { 0, 0 })), 1);

			Spec(new Quadruple(new long[] { 0, 0 }).CompareTo(
				 new Quadruple(new long[] { 0, 900 })), -1);

			Spec(new Quadruple(new long[] { 1, 0 }).CompareTo(
				 new Quadruple(new long[] { 0, 0 })), 1);

			Spec(new Quadruple(new long[] { 1, 0 }).CompareTo(
				 new Quadruple(new long[] { 900, 0 })), -1);

			Spec(new Quadruple(new long[] { 1, Int64.MaxValue }).CompareTo(
				 new Quadruple(new long[] { 0, 0 })), 1);

			Spec(new Quadruple(new long[] { 0, 0 }).CompareTo(
				 new Quadruple(new long[] { 0, Int64.MaxValue })), -1);

			Spec(new Quadruple(new long[] { 0, 0 }).CompareTo(
				 new Quadruple(new long[] { 0, Int64.MaxValue + 1 })), -1);

			Spec(new Quadruple(new long[] { 0, Int64.MaxValue }).CompareTo(
				 new Quadruple(new long[] { 0, 0 })), 1);

			Spec(new Quadruple(new long[] { 0, Int64.MaxValue + 1 }).CompareTo(
				 new Quadruple(new long[] { 0, 0 })), 1);

			Spec(new Quadruple(new long[] { 0, Int64.MaxValue }).CompareTo(
				 new Quadruple(new long[] { 0, Int64.MaxValue + 1 })), -1);

			Spec(new Quadruple(new long[] { 0, Int64.MaxValue + 1 }).CompareTo(
				 new Quadruple(new long[] { 0, Int64.MaxValue })), 1);

			Spec(new Quadruple(new long[] { 0, Int64.MaxValue }).CompareTo(
				 new Quadruple(new long[] { 0, Int64.MaxValue })), 0);

			Spec(new Quadruple(new long[] { 0, Int64.MaxValue + 1 }).CompareTo(
				 new Quadruple(new long[] { 0, Int64.MaxValue + 1 })), 0);

			Spec(new Quadruple(new long[] { Int64.MaxValue, 0 }).CompareTo(
				 new Quadruple(new long[] { Int64.MaxValue + 1, 0 })), 1);

			Spec(new Quadruple(new long[] { 0, 0x0FFFFFFFFFFFFFFFFL }).CompareTo(
				 new Quadruple(new long[] { 0, 0x0FFFFFFFFFFFFFFFEL })), 1);

			// This is -1 vs -2
			Spec(new Quadruple(new long[] { -1, 0x0FFFFFFFFFFFFFFFFL }).CompareTo(
				 new Quadruple(new long[] { -1, 0x0FFFFFFFFFFFFFFFEL })), 1);

			Spec(new Quadruple(new long[] { 1, 0 }).CompareTo(
				 new Quadruple(new long[] { 1, 100 })), -1);
		}
	}
}