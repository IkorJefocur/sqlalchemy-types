from datetime import timedelta, date, time, datetime, timezone
from sqlalchemy import TypeDecorator, Integer, Float, type_coerce

class Interval(TypeDecorator):

	impl = Float
	cache_ok = True

	def __init__(self, *, allow_overflow = False):
		super().__init__()
		self.allow_overflow = allow_overflow

	def process_bind_param(self, value, dialect):
		return (
			None if value is None
			else float('-inf') if self.allow_overflow and value == timedelta.min
			else float('inf') if self.allow_overflow and value == timedelta.max
			else value.total_seconds()
		)

	def process_result_value(self, value, dialect):
		try:
			return None if value is None else timedelta(seconds = value)
		except OverflowError:
			if self.allow_overflow:
				return timedelta.min if value < 0 else timedelta.max
			raise

class Date(TypeDecorator):

	impl = Integer
	cache_ok = True

	def process_bind_param(self, value, dialect):
		return (
			None if value is None
			else round(datetime.combine(value, time()).timestamp())
		)

	def process_result_value(self, value, dialect):
		return None if value is None else date.fromtimestamp(value)

class Time(TypeDecorator):

	impl = Float
	cache_ok = True

	def __init__(self, *, timezone_aware = False):
		super().__init__()
		self.timezone_aware = timezone_aware

	def process_bind_param(self, value, dialect):
		return (
			None if value is None
			else datetime.combine(date(1970, 1, 1), value).timestamp()
		)

	def process_result_value(self, value, dialect):
		return (
			None if value is None
			else datetime.fromtimestamp(
				value,
				timezone.utc if self.timezone_aware else None
			).timetz()
		)

class DateTime(TypeDecorator):

	impl = Float
	cache_ok = True

	def __init__(self, *, timezone_aware = False):
		super().__init__()
		self.timezone_aware = timezone_aware

	def process_bind_param(self, value, dialect):
		return None if value is None else value.timestamp()

	def process_result_value(self, value, dialect):
		return (
			None if value is None
			else datetime.fromtimestamp(
				value,
				timezone.utc if self.timezone_aware else None
			)
		)

	class comparator_factory(Float.Comparator):

		def __add__(self, other):
			expression = super().__add__(other)
			return type_coerce(expression, DateTime) if (
				hasattr(other, 'type')
				and isinstance(other.type, Interval)
			) else expression

		def __sub__(self, other):
			expression = super().__sub__(other)
			return (
				type_coerce(expression, DateTime)
				if isinstance(other.type, Interval)
				else type_coerce(expression, Interval)
				if isinstance(other.type, DateTime)
				else expression
			) if hasattr(other, 'type') else expression