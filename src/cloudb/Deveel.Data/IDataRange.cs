using System;
using System.IO;

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

		IDataFile GetFile(FileAccess access);

		IDataFile GetFile(Key key, FileAccess access);

		void Delete();

		void ReplicateTo(IDataRange target);
	}
}