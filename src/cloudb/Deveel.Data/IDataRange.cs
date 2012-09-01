using System;
using System.IO;

namespace Deveel.Data {
	public interface IDataRange {
		long Count { get; }

		long CurrentPosition { get; }

		Key CurrentKey { get; }


		void MoveTo(long value);

		long MoveToKeyStart();

		long MoveToNextKey();

		long MoveToPreviousKey();

		IDataFile GetCurrentFile(FileAccess access);

		IDataFile GetFile(Key key, FileAccess access);

		void Delete();

		void ReplicateTo(IDataRange target);
	}
}