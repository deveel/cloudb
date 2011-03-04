using System;
using System.Reflection;

using Deveel.Data.Net;

namespace Deveel.Data {
	internal class BasePathWrapper {
		private const string AssemblyName = "cloudbase";

		private static readonly Assembly assembly;

		static BasePathWrapper() {
			try {
				assembly = Assembly.Load(AssemblyName);
			} catch(Exception) {
			}
		}

		public static bool IsSupported {
			get { return assembly != null; }
		}

		public object CreateDbSession(NetworkClient client, string pathName) {
			if (assembly == null)
				throw new ApplicationException("The base path is not supported.");

			Type dbSessionType = assembly.GetType("Deveel.Data.DbSession");
			return Activator.CreateInstance(dbSessionType, new object[] {client, pathName});
		}

		public object CreateDbTransaction(object session) {
			if (session == null)
				throw new ArgumentNullException("session");

			MethodInfo method = session.GetType().GetMethod("CreateTransaction");
			return method.Invoke(session, null);
		}

		public bool TableExists(object transaction, string tableName) {
			if (transaction == null)
				throw new ArgumentNullException("transaction");

			MethodInfo method = transaction.GetType().GetMethod("TableExists");
			return (bool) method.Invoke(transaction, new object[] {tableName});
		}

		public bool CreateTable(object transaction, string tableName, string[] columns) {
			return CreateTable(transaction, tableName, columns, new string[0]);
		}

		public bool CreateTable(object transaction, string tableName, string[] columns, string[] indexedColumns) {
			if (transaction == null) 
				throw new ArgumentNullException("transaction");
			if (String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException("tableName");

			MethodInfo method = transaction.GetType().GetMethod("CreateTable");
			bool created = (bool) method.Invoke(transaction, new object[] {tableName});
			if (!created)
				return false;

			PropertyInfo schemaProp = transaction.GetType().GetProperty("Schema");
			object schema = schemaProp.GetValue(transaction, null);
			method = schema.GetType().GetMethod("AddColumn");

			for (int i = 0; i < columns.Length; i++) {
				method.Invoke(schema, new object[] {columns[i]});
			}

			method = schema.GetType().GetMethod("AddIndex");
			for (int i = 0; i < indexedColumns.Length; i++) {
				method.Invoke(schema, new object[] {indexedColumns[i]});
			}

			return true;
		}

		public void Commit(object transaction) {
			if (transaction == null) 
				throw new ArgumentNullException("transaction");

			MethodInfo method = transaction.GetType().GetMethod("Commit");
			method.Invoke(transaction, null);
		}

		public void Insert(object transaction, string tableName, string key, string value) {
			Insert(transaction, tableName, new string[] {key}, new string[] {value});
		}

		public void Insert(object transaction, string tableName, string[] keys, string[] values) {
			if (transaction == null) 
				throw new ArgumentNullException("transaction");
			if (String.IsNullOrEmpty(tableName))
				throw new ArgumentNullException("tableName");

			MethodInfo method = transaction.GetType().GetMethod("GetTable");
			object table = method.Invoke(transaction, new object[] { tableName });

			method = table.GetType().GetMethod("NewRow");
			object row = method.Invoke(table, null);

			method = row.GetType().GetMethod("SetValue");

			for (int i = 0; i < keys.Length; i++) {
				method.Invoke(row, new object[] {keys[i], values[i]});
			}

			method = table.GetType().GetMethod("Insert");
			method.Invoke(table, new object[] {row});
		}
	}
}