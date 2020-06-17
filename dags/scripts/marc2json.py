#!/usr/bin/env python
from pymarc import XmlHandler, parse_xml
import json
import os


class Writer(object):
    """Base Writer object. Based on pymarc.Writer"""

    def __init__(self, file_handle):
        """Init."""
        self.file_handle = file_handle

    def write(self, record):
        """Write."""

        if len(record) == 0:
            return

    def close(self, close_fh=True):
        """Closes the writer.
        If close_fh is False close will also close the underlying file handle
        that was passed in to the constructor. The default is True.
        """

        if close_fh:
            self.file_handle.close()
        self.file_handle = None


class JSONCreate(Writer):
    """
        A class for writing objects to a JSON file
        (Based on pymarc.JSONWriter)
    """

    def __init__(self, file_handle):
        """You need to pass in a text file like object."""
        super(JSONCreate, self).__init__(file_handle)
        self.write_count = 0
        self.file_handle.write("[")

    def write(self, record):
        """Writes an object."""
        Writer.write(self, record)

        if self.write_count > 0:
            self.file_handle.write(",")
        json.dump(record, self.file_handle, separators=(",", ":"))
        self.write_count += 1

    def close(self, close_fh=True):
        """Closes the writer.

        If close_fh is False close will also close the underlying file
        handle that was passed in to the constructor. The default is True.
        """
        self.file_handle.write("]")
        Writer.close(self, close_fh)


class ExtractXmlHandler(XmlHandler):
    def __init__(self, writer):
        XmlHandler.__init__(self)
        self.writer = writer
        self.total = 0

    def process_record(self, record):
        r = {}

        if record.leader[6] == "a" and (
            record.leader[7] == "c" or record.leader[7] == "m"
        ):
            r["title"] = record.title()
            r["author"] = record.author()
            r["isbn"] = record.isbn()
            r["publisher"] = record.publisher()
            r["pubyear"] = record.pubyear()
            locations = {}

            for f in record.location():
                if f["b"] in locations:
                    locations[f["b"]].append(f["p"])
                else:
                    locations[f["b"]] = [f["p"]]
            r["locations"] = locations

            self.writer.write(r)
            self.total = self.total + 1


def main(marc_file, catalog_output):

    if os.path.isfile(marc_file):
        if os.path.isfile(catalog_output):
            print(f"** {catalog_output} already exists")
        else:
            print(f"Processing {marc_file} to create {catalog_output}")
            with open(catalog_output, "w") as jsonfile:
                writer = JSONCreate(jsonfile)
                parse_xml(marc_file, ExtractXmlHandler(writer))
                writer.close()
    else:
        print(f"You must first download {marc_file}")


if __name__ == "__main__":
    main()
