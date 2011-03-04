using System;

namespace Deveel.Data.Net.Security {
	struct ResultInfo<T> {
		private readonly bool success;
		private readonly T value;

		public ResultInfo(bool success, T value)
			: this() {
			this.success = success;
			this.value = value;
		}

		public bool Success {
			get { return success; }
		}

		public T Value {
			get { return value; }
		}

		public bool IsTrue {
			get { return this ? true : false; }
		}

		public bool IsFalse {
			get { return this ? false : true; }
		}

		public static bool operator true(ResultInfo<T> result) {
			return result.Success;
		}

		public static bool operator false(ResultInfo<T> result) {
			return !result.Success;
		}

		public static ResultInfo<T> operator !(ResultInfo<T> result) {
			return new ResultInfo<T>(!result.Success, result.Value);
		}

		public static ResultInfo<T> operator &(ResultInfo<T> left, ResultInfo<T> right) {
			return new ResultInfo<T>(left.Success && right.Success, default(T));
		}

		public static ResultInfo<T> operator |(ResultInfo<T> left, ResultInfo<T> right) {
			return new ResultInfo<T>(left.Success || right.Success, default(T));
		}

		public static bool operator ==(ResultInfo<T> left, ResultInfo<T> right) {
			return left.Success == right.Success;
		}

		public static bool operator !=(ResultInfo<T> left, ResultInfo<T> right) {
			return left.Success != right.Success;
		}

		public static implicit operator T(ResultInfo<T> result) {
			return result.Value;
		}

		public ResultInfo<T> LogicalNot() {
			return !this;
		}

		public ResultInfo<T> BitwiseAnd(ResultInfo<T> other) {
			return this & other;
		}

		public ResultInfo<T> And(ResultInfo<T> other) {
			return this && other;
		}

		public ResultInfo<T> BitwiseOr(ResultInfo<T> other) {
			return this | other;
		}

		public ResultInfo<T> Or(ResultInfo<T> other) {
			return this || other;
		}

		public override int GetHashCode() {
			return this.Success.GetHashCode();
		}

		public override bool Equals(object obj) {
			if (obj == null)
				return false;

			if (GetType() != obj.GetType())
				return false;

			return Equals((ResultInfo<T>)obj);
		}

		private bool Equals(ResultInfo<T> other) {
			return Success == other.Success;
		}
	}
}