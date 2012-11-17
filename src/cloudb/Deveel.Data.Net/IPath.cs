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

namespace Deveel.Data.Net {
	/// <summary>
	/// Defines the functionalities to manage a <i>path</i> within a network
	/// system, coordinating the management and access of data.
	/// </summary>
	/// <remarks>
	/// A path can be seen as the concrete implementation of a model for
	/// storing, retrieving and coordinating data modification to a database.
	/// In fact, it is delegated to this interface the sanity control of the
	/// data committed to the database.
	/// <para>
	/// The aim of this interface is to provide a modular architecture, permitting
	/// the definition of structured models, verifying their logical consistency
	/// at the time of changes.
	/// </para>
	/// <para>
	/// The main function exported by this interface is the <see cref="Commit"/>
	/// function, that enforces the data integrity, accepting or rejecting change
	/// proposals: calls to it must be performed in a serial process, since proposal
	/// acceptance and failures must be known and deterministc. No coordination
	/// could be done if the current state of the path is unknown.
	/// </para>
	/// </remarks>
	public interface IPath {
		/// <summary>
		/// Initializes the state of the path, and it is employed to give
		/// birth at the first instance of the path during its lifetime.
		/// </summary>
		/// <param name="connection">The connection to a blank database that
		/// will be used to setup the state of the path.</param>
		/// <remarks>
		/// This method will be called just once during the lifetime of
		/// the path, that is at the moment of creation: it will establish
		/// the path to the root server of a network.
		/// </remarks>
		void Init(IPathConnection connection);
		
		/// <summary>
		/// Attemts to commits all the changes proposed to a given transaction,
		/// changing the state of the underlying database.
		/// </summary>
		/// <param name="connection">A connection that gives access to the latest
		/// version and historical versions of modifications proposed to the database.</param>
		/// <param name="rootNode">The address to the root node of the tree of
		/// modifications proposed.</param>
		/// <returns>
		/// If the function was successful, this returns a <see cref="DataAddress"/> 
		/// pointing to the new root node of the current version of the path.
		/// </returns>
		/// <exception cref="CommitFaultException">
		/// If the proposed set of changes was rejected.
		/// </exception>
		DataAddress Commit(IPathConnection connection, DataAddress rootNode);

		string GetStats(IPathConnection connection, DataAddress snapshot);
	}
}