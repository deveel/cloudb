using System;

using Deveel.Data.Net;

namespace Deveel.Data {
	[Handle(typeof(BasePath))]
	public sealed class BasePathMethodHandler : IMethodHandler {
		public IPathContext CreateContext(NetworkClient client, string pathName) {
			return new DbSession(client, pathName);
		}

		public MethodResponse HandleRequest(MethodRequest request) {
			if (!request.HasResourceId)
				throw new ArgumentException("The request must specify the table name.");
			
			DbTransaction transaction = (DbTransaction)request.Transaction;
			string tableName = (string) request.ResourceId;
			
			if (!transaction.TableExists(tableName))
				throw new InvalidOperationException("The table '" + tableName + "' does not exist in the current context.");
			
			MethodResponse response = request.CreateResponse();
			
			try {
				if (request.Type == MethodType.Get) {
					int rowid = -1;
					if (request.Arguments.Contains("id"))
						rowid = request.Arguments["id"].ToInt32();
					
					DbTable table = transaction.GetTable(tableName);
					if (table == null)
						throw new InvalidOperationException("Unable to retrive the table '" + tableName + "' from the context.");
					
					DbTableSchema schema = table.Schema;
					
					if (rowid == -1) {
						DbRowCursor cursor = table.GetCursor();
						while(cursor.MoveNext()) {
							response.Arguments.Add("id", cursor.Current.RowId);
						}
					} else {
						DbRow row = new DbRow(table, rowid);
						
						try {
							for (int i = 0; i < schema.ColumnCount; ) {
								response.Arguments.Add(schema.Columns[i], row.GetValue(schema.Columns[i]));
							}
						} catch (Exception e) {
							throw new Exception("Error while retrieving data from the row '" + rowid + "': probably invalid.");
						}
					}
				}
			} catch(Exception e) {
				response.Arguments.Add("ERR", e.Message);
			}
			
			return response;
		}
	}
}