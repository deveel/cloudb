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
	/// Provides functionalities for communicating with a distributed
	/// network within a coordination environment.
	/// </summary>
	public interface IPathConnection {
		/// <summary>
		/// Gets the address to the root node of the latest version
		/// of the database published.
		/// </summary>
		/// <remarks>
		/// Since the coordination mechanism operates as a serial process 
		/// and the current snapshot can only be updated by the coordination
		/// function, there is an implied certainty that the returned snapshot 
		/// will be the most current.
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="DataAddress"/> pointing to
		/// the root node, within the network, of the latest snapshot of
		/// the database.
		/// </returns>
		/// <seealso cref="SetCurrentSnapshot"/>
		DataAddress GetSnapshot();
		
		/// <summary>
		/// Given a starting and ending date, this method will return a set
		/// of root nodes to the versions of the database published within
		/// this specified time frame.
		/// </summary>
		/// <param name="start">The date of start for searching.</param>
		/// <param name="end">The end date of the searching.</param>
		/// <returns>
		/// Returns an array of <see cref="DataAddress"/> that points to the
		/// root nodes of the versions published on the database within the
		/// defined time frame.
		/// </returns>
		/// <seealso cref="GetSnapshot"/>
		DataAddress[] GetSnapshots(DateTime start, DateTime end);
		
		/// <summary>
		/// Gets all the addresses to the root nodes of the versions published
		/// on the database since the given one.
		/// </summary>
		/// <param name="rootNode">The node since where to start searching.</param>
		/// <returns>
		/// Returns an array of <see cref="DataAddress"/> pointing to the root nodes
		/// of the versions of the database published since the given node.
		/// </returns>
		DataAddress[] GetSnapshots(DataAddress rootNode);
		
		/// <summary>
		/// Sets the version, referenced by the given address, as the most recent
		/// of the database.
		/// </summary>
		/// <param name="rootNode">The address to the root node of the version.</param>
		/// <remarks>
		/// After the successful excecution of this method, any call to
		/// <see cref="GetSnapshot"/> will return the address of the given
		/// <see cref="DataAddress"/>.
		/// </remarks>
		/// <seealso cref="GetSnapshot"/>
		void Publish(DataAddress rootNode);
		
		
		/// <summary>
		/// Creates a transaction to the current version of the database
		/// from the given root node.
		/// </summary>
		/// <param name="rootNode">The address to the root node of the version
		/// of the database to which to establish the transaction.</param>
		/// <returns>
		/// Returns an instance of <see cref="ITransaction"/> that is used
		/// to interact with the database.
		/// </returns>
		ITransaction CreateTransaction(DataAddress rootNode);
		
		/// <summary>
		/// Commits all the modifications made to the given transation, persisting
		/// out to the network storage.
		/// </summary>
		/// <param name="transaction">The transaction to be committed.</param>
		/// <remarks>
		/// Once the transaction is successfully committed, the modifications
		/// applied to the database will be accessible to other clients of the
		/// network by providing the returned <see cref="DataAddress"/>, that
		/// is the pointer to the latest version of the database after the commit.
		/// <para>
		/// <b>Note</b>: this function is time and resource consuming, since it can
		/// have a great deal of information to exchange and resources to employ
		/// to store all the modifications to multiple nodes of the network.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="DataAddress"/> that points to the root node
		/// to the version of the database after the modifications.
		/// </returns>
		DataAddress CommitTransaction(ITransaction transaction);
	}
}