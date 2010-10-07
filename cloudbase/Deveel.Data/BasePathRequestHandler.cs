using System;

using Deveel.Data.Net;
using Deveel.Data.Net.Client;

namespace Deveel.Data {
	[Handle(typeof(BasePath))]
	public sealed class BasePathRequestHandler : IRequestHandler {
		private static MessageResponse HandleRestRequest(ClientMessageRequest request) {
			if (!request.HasResourceId)
				throw new ArgumentException("The request must specify the table name.");

			DbTransaction transaction = (DbTransaction)request.Transaction;
			string tableName = (string)request.ResourceId;

			MessageResponse response = null;

			try {
				if (!transaction.TableExists(tableName)) {
					response = request.CreateResponse("error");
					response.Code = MessageResponseCode.NotFound;
					response.Arguments.Add("Message", "Table '" + tableName + "' not found within the current context.");
					return response;
				}

				DbTable table = transaction.GetTable(tableName);
				if (table == null) {
					response = request.CreateResponse("error");
					response.Code = MessageResponseCode.NotFound;
					response.Arguments.Add("message", "Table '" + tableName + "' not found within the current context.");
					return response;
				}

				if (request.RequestType == RequestType.Get) {
					long rowid = -1;
					if (request.HasItemId)
						rowid = Convert.ToInt64(request.ItemId);

					DbTableSchema schema = table.Schema;

					if (rowid == -1) {
						response = request.CreateResponse("table");
						response.Attributes["name"] = tableName;
						response.Attributes["rows"] = table.RowCount;
						response.Attributes["columns"] = schema.ColumnCount;

						for (int i = 0; i < schema.ColumnCount; i++) {
							string columnName = schema.Columns[i];
							MessageArgument arg = response.Arguments.Add("column", null);
							arg.Attributes["name"] = columnName;
							arg.Attributes["indexed"] = schema.IsColumnIndexed(columnName);
						}

						DbRowCursor cursor = table.GetCursor();
						while (cursor.MoveNext()) {
							MessageArgument argument = response.Arguments.Add("row", null);
							argument.SetId("id", cursor.CurrentRowId);
						}
					} else {
						DbRow row = new DbRow(table, rowid);

						response = request.CreateResponse(tableName);
						response.Attributes["id"] = rowid;

						try {
							for (int i = 0; i < schema.ColumnCount; i++) {
								response.Arguments.Add(schema.Columns[i], row.GetValue(schema.Columns[i]));
							}

							response.Code = MessageResponseCode.Success;
						} catch (Exception) {
							response.Code = MessageResponseCode.Error;
							response.Arguments.Add("message", "Error while retrieving the row '" + rowid + "' from the table '" + tableName + "'.");
						}
					}
				} else if (request.RequestType == RequestType.Delete) {
					if (!request.HasItemId) {
						response = request.CreateResponse("error");
						response.Code = MessageResponseCode.NotFound;
						return response;
					}

					long rowid = Convert.ToInt64(request.ItemId);
					if (!table.RowExists(rowid)) {
						response = request.CreateResponse("error");
						response.Arguments.Add("message", "The row '" + rowid + "' was not indexed in the table '" + tableName + "'.");
						response.Code = MessageResponseCode.NotFound;
						return response;
					}

					table.Delete(rowid);
					response = request.CreateResponse();
					response.Code = MessageResponseCode.Success;
				} else if (request.RequestType == RequestType.Post) {
					DbRow row;

					try {
						row = BuildDbRow(table, request);
					} catch (Exception e) {
						response = request.CreateResponse("error");
						response.Code = MessageResponseCode.UnsupportedFormat;
						response.Arguments.Add("message", e.Message);
						return response;
					}

					table.Insert(row);
					response = request.CreateResponse("row");
					response.Attributes["id"] = row.RowId;
					response.Code = MessageResponseCode.Success;
				} else if (request.RequestType == RequestType.Put) {
					DbRow row;

					try {
						row = BuildDbRow(table, request);
					} catch (Exception e) {
						response = request.CreateResponse("error");
						response.Code = MessageResponseCode.UnsupportedFormat;
						response.Arguments.Add("message", e.Message);
						return response;
					}

					if (row.RowId == -1) {
						response = request.CreateResponse("error");
						response.Code = MessageResponseCode.UnsupportedFormat;
						response.Arguments.Add("message", "The id of the row to update was not specified.");
						return response;
					}

					table.Update(row);
					response = request.CreateResponse("row");
					response.Attributes["id"] = row.RowId;
					response.Code = MessageResponseCode.Success;
				}
			} catch (Exception e) {
				response = request.CreateResponse("error");
				response.Code = MessageResponseCode.Error;
				response.Arguments.Add("ERR", e.Message);
			}

			return response;			
		}

		public IPathContext CreateContext(NetworkClient client, string pathName) {
			return new DbSession(client, pathName);
		}

		public bool CanHandleClientType(string clientType) {
			return String.Compare(clientType, "rest", true) == 0;
		}

		public MessageResponse HandleRequest(ClientMessageRequest request) {
			if (request.IsRestClient)
				return HandleRestRequest(request);

			throw new InvalidOperationException();
		}

		private static DbRow BuildDbRow(DbTable table, ClientMessageRequest request) {
			long rowid = -1;
			if (request.HasItemId)
				rowid = Convert.ToInt64(request.ItemId);

			DbRow row = new DbRow(table, rowid);

			foreach(MessageArgument argument in request.Arguments) {
				row.SetValue(argument.Name, argument.ToString());
			}

			return row;
		}
	}
}