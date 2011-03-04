using System;
using System.Collections.Generic;

namespace Deveel.Data {
	public interface IInteractiveEnumerator<T> :  IEnumerator<T> {
		void Remove();
	}
}