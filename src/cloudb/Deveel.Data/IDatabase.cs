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
	/// Defines the contacts for a transactional database,
	/// providing functionalities to create, publish and dispose
	/// transaction objects.
	/// </summary>
	public interface IDatabase : IDisposable {
		/// <summary>
		/// Creates a transaction that encapsulates the latest snapshot
		/// of the current state of the database, providing an interface
		/// to interoperate with the underlying system in an isolated
		/// context.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: The transaction created at this point will not be
		/// thread-safe and it will be the responsibility of the implementations
		/// to enforce concurrency.
		/// </remarks>
		/// <seealso cref="ITransaction" />
		ITransaction CreateTransaction();

		/// <summary>
		/// Commits the modifications made into a previously created transaction
		/// (by calling <see cref="CreateTransaction" />) into the database.
		/// </summary>
		/// <param name="transaction">The transaction object containing the modifications
		/// to be committed to the database.</param>
		/// <remarks>
		/// The transaction to be published must always be the latest snapshot of the
		/// database, otherwise the process will fail.
		/// <para>
		/// It is responsibility of the underlying implementations (and eventually dedicated
		/// utilities) to merge older versions of data changes into the latest transaction
		/// active.
		/// </para>
		/// </remarks>
		void Publish(ITransaction transaction);

		/// <summary>
		/// Call to this method disposes a defined transaction.
		/// </summary>
		/// <param name="transaction">The transaction object to dispose.</param>
		/// <remarks>
		/// The logic of this method has a double behavior:
		/// <list type="bullet">
		/// <item>If the <paramref name="transaction"/> specified was already
		/// published, it will be marked <c>out of scope</c> and the resources
		/// used will be reclaimed;</item>
		/// <item>If the <paramref name="transaction"/> was not already published, 
		/// this will delete all data created within the transaction scope.</item>
		/// </list>
		/// </remarks>
		void Dispose(ITransaction transaction);

		/// <summary>
		/// Synchronizes all the data changes operated on the database to
		/// the underlying media, where supported.
		/// </summary>
		/// <remarks>
		/// Calling this method will establish a fizxed point on the history
		/// of the database where all data will be consistent and stored.
		/// This means that every modification made to it by invocations to
		/// <see cref="Publish"/> will be made stable.
		/// </remarks>
		void CheckPoint();
	}
}