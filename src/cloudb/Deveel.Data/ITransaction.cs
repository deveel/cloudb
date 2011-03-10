using System;
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// A transaction is an isolated snapshot view of the database that
	/// generated it that can be used to interoperate with the underlying
	/// database in an isolated context.
	/// </summary>
	/// <remarks>
	/// Any change made within the context of the transaction will be persisted
	/// on the underlying database once passed to the <see cref="IDatabase.Publish"/>
	/// method.
	/// <para>
	/// <b>Note</b>: Implementations of this interface will not be thread-safe,
	/// nor the <see cref="DataFile"/> created by this object.
	/// </para>
	/// </remarks>
	public interface ITransaction : IDisposable {
		/// <summary>
		/// Gets a file within the transaction context identified by the 
		/// specified key.
		/// </summary>
		/// <param name="key">The key that uniquely identifies the file within the
		/// transaction context.</param>
		/// <param name="access">Defines the access mode to the file.</param>
		/// <remarks>
		/// This method can be called multiple times to generate multiple
		/// files each with its positional state.
		/// <para>
		/// If two (or more) files identified by the same key are accessed, and
		/// one (or more) of them is modified, the modifications will be reflected
		/// to the other one(s) accordingly to the implementation specifications.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="DataFile"/> that allows external systems
		/// to access or modify the underlying data within the context of the
		/// current transaction.
		/// </returns>
		IDataFile GetFile(Key key, FileAccess access);
		
		/// <summary>
		/// This method is a convenience that indicates the transaction object the
		/// files identified by the given keys have to be pre-fetched.
		/// </summary>
		/// <param name="keys">The collection of keys identifying the data files
		/// that should be pre-fetched by the transaction.</param>
		/// <remarks>
		/// This method is intended to reduce the latency among a network, when
		/// a list of resources is known to be shortly accessed.
		/// <para>
		/// Implementations of this functionality are optionals.
		/// </para>
		/// </remarks>
		void PreFetchKeys(Key[] keys);

		IDataRange GetRange(Key minKey, Key maxKey);

		IDataRange GetRange();
	}
}