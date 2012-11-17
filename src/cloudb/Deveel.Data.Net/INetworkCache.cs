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
using System.Collections.Generic;

namespace Deveel.Data.Net {
	public interface INetworkCache {
		void SetNode(DataAddress address, ITreeNode node);

		ITreeNode GetNode(DataAddress address);

		void RemoveNode(DataAddress address);


		PathInfo GetPathInfo(String pathName);

		void SetPathInfo(String pathName, PathInfo pathInfo);


		IList<BlockServerElement> GetServersWithBlock(BlockId blockId);

		void SetServersForBlock(BlockId blockId, IList<BlockServerElement> servers, int ttlHint);

		void RemoveServersWithBlock(BlockId blockId);

	}
}