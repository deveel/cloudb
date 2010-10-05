using System;
using System.Collections.Generic;

using Deveel.Data.Net;
using Deveel.Data.Net.Client;

namespace Deveel.Data {
	[Handle(typeof(BasePath))]
	public sealed class BasePathRequestHandler : IRequestHandler {
		public IPathContext CreateContext(NetworkClient client, string pathName) {
			return new DbSession(client, pathName);
		}

		public void OnBeforeRequest(ActionRequest request, IDictionary<string, string> args) {
			//TODO: process it better
			foreach(KeyValuePair<string, string> pair in args) {
				request.Arguments.Add(pair.Key, pair.Value);
			}
		}

		public ActionResponse HandleRequest(ActionRequest request) {
			if (!request.HasResourceId)
				throw new ArgumentException("The request must specify the table name.");
			
			DbTransaction transaction = (DbTransaction)request.Transaction;
			string tableName = (string) request.ResourceId;

			ActionResponse response = request.CreateResponse();
			
			try {
				if (!transaction.TableExists(tableName)) {
					response.Code = ActionResponseCode.NotFound;
					response.Arguments.Add("ERR", "Table '" + tableName + "' not found within the current context.");
					return response;
				}

				DbTable table = transaction.GetTable(tableName);
				if (table == null) {
					response.Code = ActionResponseCode.NotFound;
					response.Arguments.Add("ERR", "Table '" + tableName + "' not found within the current context.");
					return response;
				}

				if (request.Type == RequestType.Get) {
					long rowid = -1;
					if (request.Arguments.Contains("id"))
						rowid = request.Arguments["id"].ToInt64();
					
					DbTableSchema schema = table.Schema;
					
					if (rowid == -1) {
						DbRowCursor cursor = table.GetCursor();
						while(cursor.MoveNext()) {
							response.Arguments.Add("rowid", cursor.CurrentRowId);
						}
					} else {
						DbRow row = new DbRow(table, rowid);
						
						try {
							for (int i = 0; i < schema.ColumnCount; i++) {
								response.Arguments.Add(schema.Columns[i], row.GetValue(schema.Columns[i]));
							}

							response.Code = ActionResponseCode.Success;
						} catch (Exception) {
							response.Code = ActionResponseCode.Error;
							response.Arguments.Add("ERR", "Error while retrieving the row '" + rowid + "' from the table '" + tableName + "'.");
						}
					}
				} else if (request.Type == RequestType.Delete) {
					if (!request.Arguments.Contains("id")) {
						response.Code = ActionResponseCode.NotFound;
						return response;
					}

					long rowid = request.Arguments["id"].ToInt64();
					if (!table.RowExists(rowid)) {
						response.Arguments.Add("ERR", "The row '" + rowid + "' was not indexed in the table '" + tableName + "'.");
						response.Code = ActionResponseCode.NotFound;
						return response;
					}

					table.Delete(rowid);
					response.Code = ActionResponseCode.Success;
				} else if (request.Type == RequestType.Post) {
					DbRow row;

					try {
						row = BuildDbRow(table, request);
					} catch(Exception e) {
						response.Code = ActionResponseCode.UnsupportedFormat;
						response.Arguments.Add("ERR", e.Message);
						return response;
					}

					table.Insert(row);
					response.Code = ActionResponseCode.Success;
				} else if (request.Type == RequestType.Put) {
					DbRow row;

					try {
						row = BuildDbRow(table, request);
					} catch(Exception e) {
						response.Code = ActionResponseCode.UnsupportedFormat;
						response.Arguments.Add("ERR", e.Message);
						return response;
					}

					if (row.RowId == -1) {
						response.Code = ActionResponseCode.UnsupportedFormat;
						response.Arguments.Add("ERR", "The id of the row to update was not specified.");
						return response;
					}

					table.Update(row);
					response.Code = ActionResponseCode.Success;
				}
			} catch(Exception e) {
				response.Code = ActionResponseCode.Error;
				response.Arguments.Add("ERR", e.Message);
			}
			
			return response;
		}

		private static DbRow BuildDbRow(DbTable table, ActionRequest request) {
			long rowid = -1;
			if (request.Arguments.Contains("id"))
				rowid = request.Arguments["id"].ToInt64();

			DbRow row = new DbRow(table, rowid);

			return row;
		}
	}
}