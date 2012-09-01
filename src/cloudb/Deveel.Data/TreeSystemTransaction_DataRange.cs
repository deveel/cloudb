using System;
using System.IO;

namespace Deveel.Data {
	public partial class TreeSystemTransaction {
		public class DataRange : IDataRange {
			private readonly TreeSystemTransaction transaction;

			// The lower and upper bounds of the range
			private readonly Key lowerKey;
			private readonly Key upperKey;

			// The current absolute position
			private long p;

			// The current version of the bounds information.  If it is out of date
			// it must be updated.
			private long version;
			// The current absolute start position
			private long start;
			// The current absolute position (changes when modification happens)
			private long end;

			// Tree stack
			private readonly TreeSystemStack stack;

			internal DataRange(TreeSystemTransaction transaction, Key lowerKey, Key upperKey) {
				stack = new TreeSystemStack(transaction);
				this.lowerKey = transaction.PreviousKeyOrder(lowerKey);
				this.transaction = transaction;
				this.upperKey = upperKey;
				p = 0;

				version = -1;
				start = -1;
				end = -1;
			}

			public TreeSystemTransaction Transaction {
				get { return transaction; }
			}

			public ITreeSystem TreeSystem {
				get { return transaction.TreeSystem; }
			}

			public long Count {
				get {
					transaction.CheckErrorState();
					try {
						EnsureCorrectBounds();
						return end - start;

					} catch (IOException e) {
						throw transaction.HandleIOException(e);
					} catch (OutOfMemoryException e) {
						throw transaction.HandleMemoryException(e);
					}
				}
			}

			public long CurrentPosition {
				get { return p; }
			}

			public Key CurrentKey {
				get {
					transaction.CheckErrorState();
					try {
						EnsureCorrectBounds();
						CheckAccessSize(1);

						stack.SetupForPosition(Key.Tail, start + p);
						return stack.CurrentLeafKey;
					} catch (IOException e) {
						throw transaction.HandleIOException(e);
					} catch (OutOfMemoryException e) {
						throw transaction.HandleMemoryException(e);
					}

				}
			}

			private void EnsureCorrectBounds() {
				if (transaction.updateVersion > version) {
					// If version is -1, we force a key position lookup. Version is -1
					// when the range is created or it undergoes a large structural change.
					if (version == -1) {
						// Calculate absolute upper bound,
						end = transaction.AbsKeyEndPosition(upperKey);
						// Calculate the lower bound,
						start = transaction.AbsKeyEndPosition(lowerKey);
					} else {
						if (upperKey.CompareTo(transaction.lowestSizeChangedKey) >= 0) {
							// Calculate absolute upper bound,
							end = transaction.AbsKeyEndPosition(upperKey);
						}
						if (lowerKey.CompareTo(transaction.lowestSizeChangedKey) > 0) {
							// Calculate the lower bound,
							start = transaction.AbsKeyEndPosition(lowerKey);
						}
					}
					// Reset the stack and set the version to the most recent
					stack.Reset();
					version = transaction.updateVersion;
				}
			}

			private void CheckAccessSize(int len) {
				if (p < 0 || p > (end - start - len)) {
					String msg = String.Format("Position out of bounds (p = {0}, size = {1}, read_len = {2})",
					                           p, end - start, len);
					throw new IndexOutOfRangeException(msg);
				}
			}

			private void InitWrite() {
				// Generate exception if the backed transaction is read-only.
				if (transaction.readOnly) {
					throw new ApplicationException("Read only transaction.");
				}

				// On writing, we update the versions
				if (version >= 0) {
					++version;
				}
				++transaction.updateVersion;
			}


			public void MoveTo(long value) {
				p = value;
			}

			public long MoveToKeyStart() {
				transaction.CheckErrorState();

				try {
					EnsureCorrectBounds();
					CheckAccessSize(1);

					stack.SetupForPosition(Key.Tail, start + p);
					Key curKey = stack.CurrentLeafKey;
					long startOfCur = transaction.AbsKeyEndPosition(transaction.PreviousKeyOrder(curKey)) - start;
					p = startOfCur;
					return p;
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public long MoveToNextKey() {
				transaction.CheckErrorState();
				try {
					EnsureCorrectBounds();
					CheckAccessSize(1);

					stack.SetupForPosition(Key.Tail, start + p);
					Key curKey = stack.CurrentLeafKey;
					long startOfNext = transaction.AbsKeyEndPosition(curKey) - start;
					p = startOfNext;
					return p;
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public long MoveToPreviousKey() {
				transaction.CheckErrorState();
				try {
					EnsureCorrectBounds();
					CheckAccessSize(0);

					// TODO: This seems rather complicated. Any way to simplify?

					// Special case, if we are at the end,
					long startOfCur;
					if (p == (end - start)) {
						startOfCur = p;
					}
						//
					else {
						stack.SetupForPosition(Key.Tail, start + p);
						Key curKey = stack.CurrentLeafKey;
						startOfCur = transaction.AbsKeyEndPosition(transaction.PreviousKeyOrder(curKey)) - start;
					}
					// If at the start then we can't go to previous,
					if (startOfCur == 0) {
						throw new IndexOutOfRangeException("On first key");
					}
					// Decrease the pointer and find the key and first position of that
					--startOfCur;
					stack.SetupForPosition(Key.Tail, start + startOfCur);
					Key prevKey = stack.CurrentLeafKey;
					long startOfPrev = transaction.AbsKeyEndPosition(transaction.PreviousKeyOrder(prevKey)) - start;

					p = startOfPrev;
					return p;

				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public IDataFile GetCurrentFile(FileAccess access) {
				transaction.CheckErrorState();
				try {
					EnsureCorrectBounds();
					CheckAccessSize(1);

					stack.SetupForPosition(Key.Tail, start + p);
					Key curKey = stack.CurrentLeafKey;

					return transaction.GetFile(curKey, access);
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public IDataFile GetFile(Key key, FileAccess access) {
				transaction.CheckErrorState();
				try {

					// Check the key is within range,
					if (key.CompareTo(lowerKey) < 0 ||
					    key.CompareTo(upperKey) > 0) {
						throw new IndexOutOfRangeException("Key out of bounds");
					}

					return GetFile(key, access);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void Delete() {
				transaction.CheckErrorState();
				try {
					InitWrite();
					EnsureCorrectBounds();

					if (end > start) {
						// Remove the data,
						transaction.RemoveAbsoluteBounds(start, end);
					}
					if (end < start) {
						// Should ever happen?
						throw new ApplicationException("end < start");
					}

					transaction.FlushCache();
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void ReplicateTo(IDataRange target) {
				if (target is DataRange) {
					// If the tree systems are different we fall back
					DataRange tTarget = (DataRange) target;
					if (TreeSystem == tTarget.TreeSystem) {
						// Fail condition (same transaction),
						if (tTarget.Transaction == Transaction)
							throw new ArgumentException("'ReplicateTo' on the same transaction");

						// Ok, different transaction, same tree system source, both
						// TranDataRange objects, so we can do an efficient tree copy.

						// TODO:
					}
				}

				// The fallback method,
				// This uses the standard API to replicate all the keys in the target
				// range.
				// Note that if the target can't contain the keys because they fall
				//  outside of its bound then the exception comes from the target.
				target.Delete();
				long sz = Count;
				long pos = 0;
				while (pos < sz) {
					MoveTo(pos);
					Key key = CurrentKey;
					IDataFile df = GetCurrentFile(FileAccess.Read);
					IDataFile targetDf = target.GetFile(key, FileAccess.Write);
					df.ReplicateTo(targetDf);
					pos = MoveToNextKey();
				}
			}
		}
	}
}