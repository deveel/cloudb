//
//    This file is part of Deveel in The  Cloud (CloudB).
//
//    CloudB is free software: you can redistribute it and/or modify
//    it under the terms of the GNU Lesser General Public License as 
//    published by the Free Software Foundation, either version 3 of 
//    the License, or (at your option) any later version.
//
//    CloudB is distributed in the hope that it will be useful, but 
//    WITHOUT ANY WARRANTY; without even the implied warranty of 
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
//    GNU Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public License
//    along with CloudB. If not, see <http://www.gnu.org/licenses/>.
//

using System;

namespace Deveel.Data {
	/// <summary>
	/// Represents a single node in a tree system.
	/// </summary>
	public interface ITreeNode : IDisposable {
		/// <summary>
		/// Gets the identificator of the node within the tree.
		/// </summary>
		/// <remarks>
		/// If this number is smaller than 0, this means the node
		/// is resident in the memory and it is possible to change its
		/// contents (mutable). Otherwise, if this value is greater or 
		/// equalthan 0 this means it is in the store and immutable.
		/// </remarks>
		NodeId Id { get; }

		/// <summary>
		/// Gets an estimation of the amount of memory needed by this 
		/// object to be managed by the process.
		/// </summary>
		/// <remarks>
		/// This value is used to determine the consumption of memory
		/// in the cache, and should be accurate.
		/// </remarks>
		long MemoryAmount { get; }
	}
}