import argparse
import csv
import os
import re
import sys

import calendar
import datetime
import time


class imhotep_helper:
	DEFAULT_GMT_OFFSET = -6
	MAX_FIELD_LENGTH = pow(2, 16) - 1

	class field_type:
		INT = 1
		STRING = 2

	date_formats = (
		'%Y-%m-%d',
		'%Y-%m-%d %H:%M:%S',
		'%m/%d/%Y',
		'%m/%d/%Y %H:%M:%S',
		'%a %b %d %H:%M:%S %Y',
		'%Y-%m-%dT%H:%M:%S',
	)
	date_formats_string = '"' + '", "'.join(date_formats) + '"'
	date_formats_string = re.sub(r'%', r'%%', date_formats_string)

	class imhotep_csv_dialect(csv.Dialect):
		delimiter = ','
		doublequote = False
		escapechar = '\\'
		lineterminator = '\n'
		quoting = csv.QUOTE_MINIMAL
		quotechar = '"'

	class imhotep_tsv_dialect(csv.Dialect):
		delimiter = '\t'
		doublequote = False
		escapechar = None
		lineterminator = '\n'
		quoting = csv.QUOTE_NONE

	def __init__(self, args):
		self.args = args

		# imhotep uses GMT -6. subtract the GMT offset to get a time that 
		# imhotep will treat as expected.
		print 'Using GMT offset %s' % self.args.offset
		self.offset = -self.args.offset * 60 * 60

		self.min_timestamp = pow(2, 32) - 1
		self.max_timestamp = 0

		self.header = None

		self.tsvfile = False
		self.detect_filetype()

		self.outfile = None
		self.outwriter = None

		self.int_counts = []
		self.string_counts = []

		self.total_lines = 0

		if self.args.format:
			self.date_formats = (self.args.format,)

	def detect_filetype(self):
		with open(self.args.datafile) as f:
			header = f.next()
			csvsplit = header.split(',')
			tsvsplit = header.split('\t')

			if len(tsvsplit) > len(csvsplit):
				print 'detected tsv file type'
				self.tsvfile = True
			else:
				print 'detected csv file type'

	def convert_field_name(self, name):
		# Use field names that contain uppercase A-Z, lowercase a-z, digits, or _ (underscore).
		# A field name cannot start with a digit.
		new_name = re.sub(r'[^A-Za-z0-9_]', r'_', name)
		new_name = re.sub(r'^[0-9]', r'_', new_name)

		if name != new_name:
			print 'WARN [0]: column "%s" is invalid.' % name
			if self.do_conversion():
				print '> Renaming to "%s"' % new_name
			else:
				print '> Suggest renaming to "%s"' % new_name

		return new_name

	def convert_header(self):
		for i in range(0, len(self.header)):
			self.header[i] = self.convert_field_name(self.header[i])

	# see http://aboutsimon.com/2013/06/05/datetime-hell-time-zone-aware-to-unix-timestamp/
	def to_unix_time(self, time_str):
		if time_str:
			for format in self.date_formats:
				try:
					timestamp = calendar.timegm(time.strptime(time_str, format))
				except ValueError:
					timestamp = 0

				if timestamp:
					# adjust by offset
					timestamp += self.offset

					self.min_timestamp = min(timestamp, self.min_timestamp)
					self.max_timestamp = max(timestamp, self.max_timestamp)

					return str(timestamp)

		return ''

	def rename_file(self, old_filename):
		# yyyyMMdd.HH

		# filename timestamps actually need to reflect the original time
		max_timestamp = self.max_timestamp - self.offset
		min_timestamp = self.min_timestamp - self.offset

		# now add 1 hour to end time to round up
		max_timestamp = max_timestamp + 60 * 60
		end_date = datetime.datetime.utcfromtimestamp(max_timestamp).strftime('%Y%m%d.%H')
		start_date = datetime.datetime.utcfromtimestamp(min_timestamp).strftime('%Y%m%d.%H')

		stripped_filename = re.sub(r'__new__', r'', self.args.datafile)
		stripped_filename = re.sub(r'[^\-\.a-zA-Z]', r'', stripped_filename)

		new_filename = '%s%s-%s_%s' % (self.args.prefix, start_date, end_date, stripped_filename)

		print ''
		if self.do_conversion():
			print 'renaming temp file "%s" to "%s"' % (old_filename, new_filename)
			os.rename(old_filename, new_filename)
		else:
			print 'Suggest renaming file from "%s" to "%s"' % (self.args.datafile, new_filename)

	def detect_timestamp_column(self):
		for i, field in enumerate(self.header):
			if field == 'time' or field == 'unixtime':
				self.args.index = i
				break

	def do_conversion(self):
		return self.args.convert and not self.args.lint

	def check_values(self, line_number, row):
		new_row = list(row)

		# convert datetime column to unix timestamp
		if self.args.index >= 0:
			new_row[self.args.index] = self.to_unix_time(new_row[self.args.index])

		for i, val in enumerate(new_row):
			# count ints & strings for non-timestamp fields
			if i != self.args.index and len(val):
				try:
					int_val = int(val)
					self.int_counts[i] += 1
				except ValueError:
					self.string_counts[i] += 1
					if i == self.args.nonint:
						print 'WARN [%s]: non-integer field value "%s"' % (line_number, val)

			# field values can't exceed 64k
			if len(val) > self.MAX_FIELD_LENGTH:
				print 'WARN [%s]: max field length %s exceeded' % line_number
				if self.args.convert:
					print '> Truncating...'
					new_row[i] = val[0:self.MAX_FIELD_LENGTH]

			# tsv files do not support " or \ chars
			if self.tsvfile:
				if val.find('"') != -1:
					print 'WARN [%s]: Found a quote character (") in entry. TSVs do not support quoting. Entry: "%s"' % (line_number, val)
					if self.args.convert:
						new_val = re.sub(r'"', r'', val)
						print '> Converting to: "%s"' % new_val
						new_row[i] = new_val
				if val.find('\\') != -1:
					print 'WARN [%s]: Found an escape character (\\) in entry. TSVs do not support quoting. Entry: "%s"' % (line_number, val)
					if self.args.convert:
						new_val = re.sub(r'\\', r'', val)
						print '> Converting to: "%s"' % new_val
						new_row[i] = new_val

		return new_row

	def row_valid(self, row):
		return len(row) > self.args.index

	def row_type(self, index):
		int_percent = float(self.int_counts[index]) / self.total_lines
		string_percent = float(self.string_counts[index]) / self.total_lines

		print ''
		print 'Field "%s":' % self.header[index]

		if index == self.args.index:
			print '> Will be converted to unixtime'
		else:
			print '- %.1f%% int values' % (int_percent * 100)
			print '- %.1f%% string values' % (string_percent * 100)

			if int_percent >= 0.2 and string_percent <= 0.1:
				print '> Will be treated as an int field. Non-int values will be discarded.'
				#return imhotep_helper.field_type.INT
			else:
				print '> Will be treated as a string field.'
				#return imhotep_helper.field_type.STRING

	def calc_stats(self):
		print ''
		print 'SUMMARY STATS:'
		print ''
		print 'Total records: %s' % self.total_lines
		for i in range(len(self.header)):
			self.row_type(i)

	def init_nonint(self):
		if self.args.nonint:
			# should be int column number to check.
			# if castable to int, use that
			try:
				self.args.nonint = int(self.args.nonint)
			except ValueError:
				# otherwise should be column name
				try:
					self.args.nonint = self.header.index(self.args.nonint)
				except ValueError:
					print 'ERROR: nonint argument must be a valid column index, or name (one of %s)' % self.header
					raise

	def init_header(self):
		if self.args.index == -1:
			self.detect_timestamp_column()
		else:
			# ensure timestamp column is named correctly
			if self.header[self.args.index] != 'time' and self.header[self.args.index] != 'unixtime':
				self.header[self.args.index] = 'time'

		# initialize counts
		self.int_counts = [0] * len(self.header)
		self.string_counts = [0] * len(self.header)

		self.convert_header()

		self.init_nonint()

	def check_file(self):
		new_filename = ''
		if self.do_conversion():
			new_filename = '%s__new__' % self.args.datafile
			print 'converting "%s" to temp file "%s"...' % (self.args.datafile, new_filename)

			self.outfile = open(new_filename, 'w')

			if self.tsvfile:
				self.outwriter = csv.writer(self.outfile, dialect=self.imhotep_tsv_dialect())
			else:
				self.outwriter = csv.writer(self.outfile, dialect=self.imhotep_csv_dialect())

		print ''

		with open(self.args.datafile) as csvin:
			if self.tsvfile:
				r = csv.reader(csvin, dialect=self.imhotep_tsv_dialect())
			else:
				r = csv.reader(csvin, dialect=self.imhotep_csv_dialect())

			# read header first
			self.header = list(r.next())
			self.init_header()

			if self.do_conversion():
				self.outwriter.writerow(self.header)

			for line_number, row in enumerate(r):
				if self.row_valid(row):
					self.total_lines += 1
					new_row = self.check_values(line_number, row)

					if self.do_conversion():
						self.outwriter.writerow(new_row)

		if self.do_conversion():
			self.outfile.close()

		self.calc_stats()

		self.rename_file(new_filename)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()

	parser.add_argument('datafile', nargs='?', help='filename of data to upload')

	parser.add_argument('-l', '--lint', action='store_true', help='check file for problems. if specified, overrides --convert.')
	parser.add_argument('-c', '--convert', action='store_true', help='automatically fix problems and convert')
	parser.add_argument('-n', '--nonint', type=str, help='name or index of column to display non-integer values (name must be a valid index name)')
	parser.add_argument('-i', '--index', type=int, default=-1, help='index of timestamp field')
	parser.add_argument('--prefix', type=str, default='converted_', help='prefix of converted filename. default = "converted_"')

	parser.add_argument('-f', '--format', type=str, \
		help=('format of timestamp field. defaults include %s. ' + \
		' (see https://docs.python.org/2/library/datetime.html#strftime-strptime-behavior for details)') \
		% imhotep_helper.date_formats_string)
	parser.add_argument('-o', '--offset', type=int, default=imhotep_helper.DEFAULT_GMT_OFFSET, \
		help='GMT offset of timestamps. default is %s.' % imhotep_helper.DEFAULT_GMT_OFFSET)

	args = parser.parse_args()

	if not (args.lint or args.convert):
		parser.print_help()
	else:
		ih = imhotep_helper(args)
		if ih.args.datafile:
			try:
				ih.check_file()
			except ValueError as e:
				print e


