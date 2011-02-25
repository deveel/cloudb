using System;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	/// <summary>
	/// A <see cref="IDataRange"/> is a sorted set of consecutive keys 
	/// stored in a <see cref="ITransaction"/>.
	/// </summary>
	public interface IDataRange {
		long Count { get; }

		long Position { get; set; }

		long MoveToStart();

		long MoveNext();

		long MovePrevious();

		Key CurrentKey { get; }

		DataFile GetFile(FileAccess access);

		DataFile GetFile(Key key, FileAccess access);

		void Delete();

		void ReplicateTo(IDataRange target);
	}
}