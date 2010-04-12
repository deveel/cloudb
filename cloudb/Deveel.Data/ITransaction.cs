using System;
using System.IO;

using Deveel.Data.Store;

namespace Deveel.Data {
	public interface ITransaction : IDisposable {
		DataFile GetFile(Key key, FileAccess access);
	}
}