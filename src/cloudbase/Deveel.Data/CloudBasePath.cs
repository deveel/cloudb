using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Net;

namespace Deveel.Data {
	public sealed class CloudBasePath : IPath {
		public void Init(IPathConnection connection) {
			// Get the current root,
			DataAddress currentRoot = connection.GetSnapshot();
			// Turn it into a transaction
			ITransaction transaction = connection.CreateTransaction(currentRoot);
			// Initialize the magic property set, etc
			IDataFile df = transaction.GetFile(DbTransaction.MagicKey, FileAccess.ReadWrite);
			StringDictionary magicSet = new StringDictionary(df);
			magicSet.SetValue("ob_type", "Deveel.Data.CloudBase");
			magicSet.SetValue("version", "1.0");
			// Flush and publish the change
			DataAddress finalRoot = connection.CommitTransaction(transaction);
			connection.Publish(finalRoot);
		}

		public DataAddress Commit(IPathConnection connection, DataAddress rootNode) {
			// Turn the proposal into a proposed_transaction,
			ITransaction t = connection.CreateTransaction(rootNode);
			DbTransaction proposedTransaction = new DbTransaction(null, rootNode, t);

			try {
				// Fetch the base root from the proposed_transaction log,
				TextReader reader = proposedTransaction.GetLogReader();
				string baseRootStr = reader.ReadLine();
				// If 'base_root_str' is "no base root" then it means we are commiting
				// an introduced transaction that is not an iteration of previous
				// snapshots.
				if (baseRootStr.Equals("no base root")) {

					// In which case, we publish the proposed snapshot unconditionally
					// and return.
					connection.Publish(rootNode);
					return rootNode;

				}

				DataAddress baseRoot = DataAddress.Parse(baseRootStr);

				// Find all the entries since this base
				DataAddress[] roots = connection.GetSnapshots(baseRoot);

				// If there are no roots, we can publish the proposed_transaction
				// unconditionally
				if (roots.Length == 0) {

					connection.Publish(rootNode);
					return rootNode;

				}

				// Check historical log for clashes, and if none, replay the commands
				// in the log.

				// For each previous root, we build a structure that can answer the
				// following questions;
				// * Is file [name] created, deleted or changed in this root?
				// * Is table [name] created or deleted in this root?
				// * Is table [name] structurally changed in this root?
				// * Has row [rowid] been deleted in table [name] in this root?

				RootEvents[] rootEventSet = new RootEvents[roots.Length];
				int i = 0;

				// TODO: RootEvents is pre-computed information which we could
				//   store in a local cache for some speed improvements, so we don't
				//   need to walk through through the same data multiple times.

				foreach (DataAddress root in roots) {
					RootEvents rootEvents = new RootEvents();
					rootEventSet[i] = rootEvents;
					++i;
					// Create a transaction object for this root
					ITransaction rootT = connection.CreateTransaction(root);
					DbTransaction rootTransaction = new DbTransaction(null, root, rootT);
					// Make a reader object for the log,
					TextReader rootReader = rootTransaction.GetLogReader();
					// Read the base root from this transaction,
					String baseRootParent = rootReader.ReadLine();
					// If 'bast_root_parent' is 'no base root' then it means a version
					// has been introduced that is not an iteration of previous
					// snapshots. In this case, it is not possible to merge updates
					// therefore we generate a commit fault.
					if (baseRootParent.Equals("no base root"))
						throw new CommitFaultException("Transaction history contains introduced version.");

					// Go through each log entry and determine if there's a clash,
					string rootLine = rootReader.ReadLine();
					while (rootLine != null) {
						String mfile = rootLine.Substring(2);
						// This represents a file modification,
						bool unknownCommand = false;
						if (rootLine.StartsWith("F")) {
							rootEvents.SetFileChange(mfile);
						}
							// This is a table modification,
						else if (rootLine.StartsWith("T")) {
							char c = rootLine[1];
							// If this is a table create or delete event,
							if (c == 'C' || c == 'D') {
								rootEvents.SetTableCreateOrDelete(mfile);
							}
								// This is a table structural change,
							else if (c == 'S') {
								rootEvents.SetTableStructuralChange(mfile);
							}
								// This is a table data change event,
							else if (c == 'M') {
								DbTable table = rootTransaction.GetTable(mfile);
								rootEvents.SetTableDataChange(mfile, table);
							} else {
								unknownCommand = true;
							}
						} else {
							unknownCommand = true;
						}
						if (unknownCommand) {
							throw new ApplicationException("Unknown transaction command: " + rootLine);
						}
						// Read the next log entry,
						rootLine = rootReader.ReadLine();
					}
				}

				// Now we have a set of RootEvents objects that describe what
				// happens in each previous root.

				// Now replay the events in the proposal transaction in the latest
				// transaction.

				// A transaction representing the current state,
				DataAddress currentRoot = connection.GetSnapshot();
				ITransaction currentT = connection.CreateTransaction(currentRoot);
				DbTransaction currentTransaction = new DbTransaction(null, currentRoot, currentT);
				String entry = reader.ReadLine();
				while (entry != null) {
					String mfile = entry.Substring(2);
					// If it's a file entry, we need to check the file hasn't been
					// changed in any way in any roots
					if (entry.StartsWith("F")) {
						foreach (RootEvents events in rootEventSet) {
							events.CheckFileChange(mfile);
						}

						// All checks passed, so perform the operation
						currentTransaction.ReplayFileLogEntry(entry, proposedTransaction);
					}
						// If it's a table entry,
					else if (entry.StartsWith("T")) {
						// Check that a table with this name hasn't been created, deleted
						// or modified,
						foreach (RootEvents events in rootEventSet) {
							// This fails on any event on this table, except a data change
							// (insert or delete)
							events.CheckTableMetaChange(mfile);
						}
						// The type of operation,
						char c = entry[1];
						// Is it a table structural change?
						if (c == 'S') {
							// A structural change can only happen if all the roots leave the
							// table untouched,
							foreach (RootEvents events in rootEventSet) {
								// This fails if it finds a delete event for this rowid
								events.CheckTableDataChange(mfile);
							}
						}
							// Is it a table modification command?
						else if (c == 'M') {
							// This is a table modification, we need to check the rowid
							// logs and look for possible clashes,
							// The delete set from the proposed transaction,
							DbTable proposedTable = proposedTransaction.GetTable(mfile);
							SortedIndex deleteSet = proposedTable.Deletes;
							IIndexCursor dsi = deleteSet.GetCursor();
							while (dsi.MoveNext()) {
								long rowid = dsi.Current;
								foreach (RootEvents events in rootEventSet) {
									// This fails if it finds a delete event for this rowid
									events.CheckTableDataDelete(mfile, rowid);
								}
							}
						}
						// Go through each root, if the data in the table was changed
						// by any of the roots, we set 'has_data_changes' to true;
						bool hasDataChanges = false;
						foreach (RootEvents events in rootEventSet) {
							if (events.HasTableDataChanges(mfile)) {
								hasDataChanges = true;
							}
						}

						// Ok, checks passed, so reply all the data changes on the table
						currentTransaction.ReplayTableLogEntry(entry, proposedTransaction, hasDataChanges);
					} else {
						throw new ApplicationException("Unknown transaction command: " + entry);
					}

					// Read the next log entry,
					entry = reader.ReadLine();
				}

				// Refresh the transaction log
				currentTransaction.RefreshTransactionLog();

				// Flush and publish the change
				DataAddress finalRoot = connection.CommitTransaction(currentT);
				connection.Publish(finalRoot);

				// Done.
				return finalRoot;
			} catch (IOException e) {
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		public string GetStats(IPathConnection connection, DataAddress snapshot) {
			throw new NotImplementedException();
		}

		#region RootEvents

		private sealed class RootEvents {
			// The set of all files changed in this root,
			private List<String> filesChanged;
			// The set of all tables created or deleted in this root,
			private List<String> tablesCreatedDeleted;
			// The set of all tables structurally changed in this root,
			private List<String> tablesStructural;
			// A map of table to SDBTable of all tables that were changed in the root,
			private Dictionary<String, DbTable> tableDataChanged;


			public bool HasTableDataChanges(string table) {
				if (tableDataChanged == null) {
					return false;
				}
				return tableDataChanged.ContainsKey(table);
			}

			public void SetFileChange(String fileName) {
				if (filesChanged == null) {
					filesChanged = new List<string>();
				}
				filesChanged.Add(fileName);
			}

			public void SetTableCreateOrDelete(String tableName) {
				if (tablesCreatedDeleted == null) {
					tablesCreatedDeleted = new List<string>();
				}
				tablesCreatedDeleted.Add(tableName);
			}

			public void SetTableStructuralChange(string tableName) {
				if (tablesStructural == null) {
					tablesStructural = new List<string>();
				}
				tablesStructural.Add(tableName);
			}

			public void SetTableDataChange(string tableName, DbTable table) {
				if (tableDataChanged == null) {
					tableDataChanged = new Dictionary<string, DbTable>();
				}
				tableDataChanged[tableName] = table;
			}

			public void CheckFileChange(String file) {
				if (filesChanged != null &&
				    filesChanged.Contains(file)) {
					throw new CommitFaultException(String.Format("File ''{0}'' was modified by a concurrent transaction", file));
				}
			}

			public void CheckTableDataChange(String table) {
				if (tableDataChanged != null &&
				    tableDataChanged.ContainsKey(table)) {
					throw new CommitFaultException(String.Format("Table ''{0}'' was modified by a concurrent transaction", table));
				}
			}

			public void CheckTableMetaChange(String table) {
				// Check if the table was created or deleted in this root
				if (tablesCreatedDeleted != null &&
				    tablesCreatedDeleted.Contains(table)) {
					throw new CommitFaultException(String.Format("Table ''{0}'' was modified by a concurrent transaction", table));
				}
				// Check if the table was structurally changed in this root
				if (tablesStructural != null &&
				    tablesStructural.Contains(table)) {
					throw new CommitFaultException(String.Format(
						"Table ''{0}'' was structurally modified by a concurrent transaction", table));
				}
			}

			public void CheckTableDataDelete(String table, long rowid) {
				// Is it in the modification set?
				if (tableDataChanged != null) {
					DbTable t;
					if (tableDataChanged.TryGetValue(table, out t)) {
						// Yes, so check if the given row in the modification set,
						SortedIndex deleteSet = t.Deletes;
						if (deleteSet.ContainsSortKey(rowid)) {
							// Yes, so generate a commit fault,
							throw new CommitFaultException(String.Format("Row in Table ''{0}'' was modified by a concurrent transaction",
							                                             table));
						}
					}
				}
			}
		}

		#endregion
	}
}