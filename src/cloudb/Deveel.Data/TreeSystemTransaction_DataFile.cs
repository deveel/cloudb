using System;
using System.IO;

namespace Deveel.Data {
	public partial class TreeSystemTransaction {
		private class DataFile : IDataFile {
			private readonly TreeSystemTransaction transaction;

			// The Key
			private readonly Key key;

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

			// A small buffer used for converting primitives
			private readonly byte[] convert_buffer;

			// True if locally, this transaction is read only
			private readonly bool file_read_only;

			internal DataFile(TreeSystemTransaction transaction, Key key, bool file_read_only) {
				this.stack = new TreeSystemStack(transaction);
				this.transaction = transaction;
				this.key = key;
				this.p = 0;
				this.convert_buffer = new byte[8];

				this.version = -1;
				this.file_read_only = file_read_only;
				this.start = -1;
				this.end = -1;
			}

			public TreeSystemTransaction Transaction {
				get { return transaction; }
			}

			public ITreeSystem TreeSystem {
				get { return transaction.TreeSystem; }
			}

			public long Length {
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

			public long Position {
				get { return p; }
				set { p = value; }
			}

			private void EnsureCorrectBounds() {
				if (transaction.updateVersion > version) {

					// If version is -1, we force a key position lookup.  Version is -1
					// when the file is created or it undergoes a large structural change
					// such as a copy.
					if (version == -1 || key.CompareTo(transaction.lowestSizeChangedKey) >= 0) {
						long[] bounds = transaction.GetDataFileBounds(key);
						start = bounds[0];
						end = bounds[1];
					} else {
						// If version doesn't equal -1, and this key is lower than the lowest
						// size changed key, then 'start' and 'end' should be correct.
					}
					// Reset the stack and set the version to the most recent
					stack.Reset();
					version = transaction.updateVersion;
				}
			}

			private void CheckAccessSize(int len) {
				if (p < 0 || p > (end - start - len)) {
					throw new IndexOutOfRangeException(String.Format("Position out of bounds (p = {0}, size = {1}, read_len = {2})", p,
					                                                 end - start, len));
				}
			}

			private void InitWrite() {
				// Generate exception if this is read-only.
				// Either the transaction is read only or the file is read only
				if (transaction.readOnly)
					throw new ApplicationException("Read only transaction.");
				if (file_read_only) {
					throw new ApplicationException("Read only data file.");
				}

				// On writing, we update the versions
				if (version >= 0) {
					++version;
				}
				++transaction.updateVersion;
			}

			private void UpdateLowestSizeChangedKey() {
				// Update the lowest sized changed key
				if (key.CompareTo(transaction.lowestSizeChangedKey) < 0) {
					transaction.lowestSizeChangedKey = key;
				}
			}

			private void EnsureBounds(long endPoint) {
				// The number of bytes to expand by
				long toExpandBy = endPoint - end;

				// If we need to expand,
				if (toExpandBy > 0) {
					long sizeDiff = toExpandBy;
					// Go to the end position,
					stack.SetupForPosition(key, Math.Max(start, end - 1));
					// Did we find a leaf for this key?
					if (!stack.CurrentLeafKey.Equals(key)) {
						// No, so add empty nodes after to make up the space
						stack.AddSpaceAfter(key, toExpandBy);
					} else {
						// Otherwise, try to expand the current leaf,
						toExpandBy -= stack.ExpandLeaf(toExpandBy);
						// And add nodes for the remaining
						stack.AddSpaceAfter(key, toExpandBy);
					}
					end = endPoint;

					// Update the state because this key changed the relative offset of
					// the keys ahead of it.
					UpdateLowestSizeChangedKey();
				}
			}

			private void CopyDataTo(long position, DataFile targetDataFile, long targetPosition, long size) {
				// If transactions are the same (data is being copied within the same
				// transaction context).
				TreeSystemStack targetStack;
				TreeSystemStack sourceStack;
				// Keys
				Key targetKey = targetDataFile.key;
				Key sourceKey = key;

				bool modifyPosOnShift = false;
				if (targetDataFile.Transaction == Transaction) {
					// We set the source and target stack to the same
					sourceStack = targetDataFile.stack;
					targetStack = sourceStack;
					// If same transaction and target_position is before the position we
					// set the modify_pos_on_shift boolean.  This will update the absolute
					// position when data is copied.
					modifyPosOnShift = (targetPosition <= position);
				} else {
					// Otherwise, set the target stack to the target file's stack
					sourceStack = stack;
					targetStack = targetDataFile.stack;
				}


				// Compact the key we are copying from, and in the destination,
				transaction.CompactNodeKey(sourceKey);
				targetDataFile.CompactNodeKey(targetKey);


				// The process works as follows;
				// 1. If we are not positioned at the start of a leaf, copy all data up
				//    to the next leaf to the target.
				// 2. Split the target leaf at the new position if the leaf can be
				//    split into 2 leaf nodes.
				// 3. Copy every full leaf to the target as a new leaf element.
				// 4. If there is any remaining data to copy, insert it into the target.

				// Set up for the position
				sourceStack.SetupForPosition(sourceKey, position);
				// If we aren't at the start of the leaf, then copy the data to the
				// target.
				int leafOff = sourceStack.LeafOffset;
				if (leafOff > 0) {
					// We copy the remaining data in the leaf to the target
					// The amount of data to copy from the leaf to the target
					int to_copy = (int) Math.Min(size, sourceStack.LeafSize - leafOff);
					if (to_copy > 0) {
						// Read into a buffer
						byte[] buf = new byte[to_copy];
						sourceStack.CurrentLeaf.Read(leafOff, buf, 0, to_copy);
						// Make enough room to insert this data in the target
						targetStack.ShiftData(targetKey, targetPosition, to_copy);
						// Update the position if necessary
						if (modifyPosOnShift) {
							position += to_copy;
						}
						// Write the data to the target stack
						targetStack.WriteFrom(targetKey, targetPosition, buf, 0, to_copy);
						// Increment the pointers
						position += to_copy;
						targetPosition += to_copy;
						size -= to_copy;
					}
				}

				// If this is true, the next iteration will use the byte buffer leaf copy
				// routine.  Set if a link to a node failed for whatever reason.
				bool useByteBufferCopyForNext = false;

				// The loop
				while (size > 0) {

					// We now know we are at the start of a leaf with data left to copy.
					sourceStack.SetupForPosition(sourceKey, position);
					// Lets assert that
					if (sourceStack.LeafOffset != 0) {
						throw new ApplicationException("Expected to be at the start of a leaf.");
					}

					// If the source is a heap node or we are copying less than the data
					// that's in the leaf then we use the standard shift and write.
					TreeLeaf currentLeaf = sourceStack.CurrentLeaf;
					// Check the leaf size isn't 0
					if (currentLeaf.Length <= 0) {
						throw new ApplicationException("Leaf is empty.");
					}
					// If the remaining copy is less than the size of the leaf we are
					// copying from, we just do a byte array copy
					if (useByteBufferCopyForNext || size < currentLeaf.Length) {
						// Standard copy through a byte[] buf,
						useByteBufferCopyForNext = false;
						int toCopy = (int) Math.Min(size, currentLeaf.Length);
						// Read into a buffer
						byte[] buf = new byte[toCopy];
						currentLeaf.Read(0, buf, 0, toCopy);
						// Make enough room in the target
						targetStack.ShiftData(targetKey, targetPosition, toCopy);
						if (modifyPosOnShift) {
							position += toCopy;
						}
						// Write the data and finish
						targetStack.WriteFrom(targetKey, targetPosition, buf, 0, toCopy);
						// Update pointers
						position += toCopy;
						targetPosition += toCopy;
						size -= toCopy;
					} else {
						// We need to copy a complete leaf node,
						// If the leaf is on the heap, write it out
						if (transaction.IsHeapNode(currentLeaf.Id)) {
							sourceStack.WriteLeafOnly(sourceKey);
							// And update any vars
							currentLeaf = sourceStack.CurrentLeaf;
						}

						// Ok, source current leaf isn't on the heap, and we are copying a
						// complete leaf node, so we are elegible to play with pointers to
						// copy the data.
						targetStack.SetupForPosition(targetKey, targetPosition);
						bool insertNextBefore = false;
						// Does the target key exist?
						bool targetKeyExists = targetStack.CurrentLeafKey.Equals(targetKey);
						if (targetKeyExists) {
							// If the key exists, is target_position at the end of the span?
							insertNextBefore = targetStack.LeafOffset < targetStack.CurrentLeaf.Length;
						}

						// If target isn't currently on a boundary
						if (!targetStack.IsAtEndOfKeyData &&
						    targetStack.LeafOffset != 0) {
							// If we aren't on a boundary we need to split the target leaf
							targetStack.SplitLeaf(targetKey, targetPosition);
						}
						// If the key exists we set up the position to the previous left
						// to insert the new leaf, otherwise we set it up to the default
						// position to insert.

						// Copy the leaf,
						// Try to link to this leaf
						bool linkSuccessful = TreeSystem.LinkLeaf(targetKey, currentLeaf.Id);
						// If the link was successful,
						if (linkSuccessful) {
							// Insert the leaf into the tree
							targetStack.InsertLeaf(targetKey, currentLeaf, insertNextBefore);
							// Update the pointers
							int copiedSize = currentLeaf.Length;
							// Update if we inserting stuff before
							if (modifyPosOnShift) {
								position += copiedSize;
							}
							position += copiedSize;
							targetPosition += copiedSize;
							size -= copiedSize;
						}
							// If the link was not successful,
						else {
							// We loop back and use the byte buffer copy,
							useByteBufferCopyForNext = true;
						}
					}
				}
			}

			private void CompactNodeKey(Key k) {
				transaction.CompactNodeKey(k);
			}

			private void ShiftData(long position, long shiftOffset) {
				// Make some assertions
				long endPos = position + shiftOffset;
				if (position < start || position > end) {
					throw new ApplicationException("Position is out of bounds.");
				}
				// Make sure the ending position can't be before the start
				if (endPos < start) {
					throw new ApplicationException("Can't shift to before start boundary.");
				}
				stack.ShiftData(key, position, shiftOffset);
				end += shiftOffset;
				if (end < start) {
					throw new ApplicationException("Assertion failed: end < start");
				}

				// Update the state because this key changed the relative offset of
				// the keys ahead of it.
				UpdateLowestSizeChangedKey();
			}


			public int Read(byte[] buffer, int offset, int count) {
				transaction.CheckErrorState();
				try {
					EnsureCorrectBounds();
					CheckAccessSize(count);
					stack.ReadInto(key, start + p, buffer, offset, count);
					p += count;
					return count;
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void SetLength(long value) {
				transaction.CheckErrorState();
				try {
					InitWrite();
					EnsureCorrectBounds();

					long currentSize = end - start;
					ShiftData(end, value - currentSize);

					transaction.FlushCache();
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void Shift(long offset) {
				transaction.CheckErrorState();
				try {
					InitWrite();
					EnsureCorrectBounds();
					CheckAccessSize(0);

					ShiftData(start + p, offset);

					transaction.FlushCache();
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void Delete() {
				transaction.CheckErrorState();
				try {

					InitWrite();
					EnsureCorrectBounds();

					ShiftData(end, start - end);

					transaction.FlushCache();
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void Write(byte[] buffer, int offset, int count) {
				transaction.CheckErrorState();
				try {
					InitWrite();
					EnsureCorrectBounds();
					CheckAccessSize(0);

					// Ensure that there is address space available for writing this.
					EnsureBounds(start + p + count);
					stack.WriteFrom(key, start + p, buffer, offset, count);
					p += count;

					transaction.FlushCache();
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void CopyTo(IDataFile destFile, long size) {
				transaction.CheckErrorState();
				try {

					// The actual amount of data to really copy
					size = Math.Min(Length - Position, size);
					// Return if we aren't doing anything
					if (size <= 0) {
						return;
					}

					// If the target isn't a TranDataFile then use standard byte buffer copy.
					if (!(destFile is DataFile)) {
						ByteBufferCopyTo(this, destFile, size);
						return;
					}
					// If the tree systems are different, then byte buffer copy.
					DataFile target = (DataFile)destFile;
					if (TreeSystem != target.TreeSystem) {
						ByteBufferCopyTo(this, destFile, size);
						return;
					}
					// Fail condition (same key and same transaction),
					if (target.key.Equals(key) &&
					    target.Transaction == Transaction) {
						throw new ArgumentException("Can not use 'copyTo' to copy data within a file");
					}

					// initWrite on this and target. The reason we do this is because we
					// may change the root node on either source or target.  We need to
					// initWrite on this object even though the data may not change,
					// because we may be writing out data from the heap as part of the
					// copy operation and the root node may change
					InitWrite();
					target.InitWrite();
					// Make sure internal vars are setup correctly
					EnsureCorrectBounds();
					target.EnsureCorrectBounds();
					// Remember the source and target positions
					long initSpos = Position;
					long initTpos = target.Position;
					// Ok, the target shares the same tree system, therefore we may be able
					// to optimize the copy.
					CopyDataTo(start + Position,target, target.start + target.Position, size);
					// Update the positions
					Position = initSpos + size;
					target.Position = initTpos + size;
					// Reset version to force a bound update
					this.version = -1;
					target.version = -1;
					target.UpdateLowestSizeChangedKey();
					target.Transaction.FlushCache();
				} catch (IOException e) {
					throw transaction.HandleIOException(e);
				} catch (OutOfMemoryException e) {
					throw transaction.HandleMemoryException(e);
				}
			}

			public void CopyFrom(IDataFile sourceFile, long size) {
				throw new NotImplementedException();
			}

			public void ReplicateTo(IDataFile destFile) {
				// TODO: Placeholder implementation,
				destFile.Position = 0;
				destFile.Delete();
				Position = 0;
				CopyTo(destFile, Length);
			}

			public void ReplicateFrom(IDataFile sourceFile) {
				throw new NotImplementedException();
			}
		}
	}
}