"""Data saving, loading and compressing module."""

import zlib
import json
import os


class DataIO:
    """Handles saving and compression of json data."""

    def __init__(self):
        self.compressor = self._get_data_stream_compressor()
        self.decompressor = self._get_data_stream_decompressor()
        self.compressed_data_type = ".stream"
        self.encoding_type = "utf-8"

        # Flags
        self._data_stream_append_mode = False  # Needs to be false to prevent bad overwriting of files
        self._data_stream_load_mode = False  # Set to true when loading a file

        # Meta information for file appending
        self._meta_info_dict = self._file_meta_info_dict_template()
        self._data_stream_chunk_amount = 0  # The amount of data chunks stored in the appended file
        self._data_stream_string_len = []  # The string length of each chunk of data (json format)

        # File loading
        self._load_index = 0  # Load index indicates where loading of data stream file should be resumed
        self._load_chunk_total = 0  # Number of data chunks stored in loaded file
        self._load_chunk_string_length = []  # Length of json strings of each data chunk in loaded file
        self._load_data_buffer = ''  # Data buffer storing json strings for loading process
        self._load_data_buffer_index = 0  # Index storing the index of the buffer relative to start complete data set
        self._string_length_loaded = 0  # Summed length of the data strings loaded
        self._current_chunk_to_load = 0  # Index of current chunk to load
        self._current_chunk_loaded = 0  # Index of current chunk loaded

    def add_data_to_file(self, data_set_name: str, data):
        """Takes observations, rewards, done flags and info and formats them into json files."""
        with open(data_set_name + ".json", "a", encoding=self.encoding_type) as file:
            json.dump(data, file)

    def save_data(self, data_set_name: str, data: str):
        """Save game data to a file using json. Accepts data as a json string."""
        with open(data_set_name + ".json", "w", encoding=self.encoding_type) as file:
            json.dump(data, file)

    def load_data(self, data_set_name: str):
        """Load game data from file and return data as a json string."""
        with open(data_set_name + ".json", "r", encoding=self.encoding_type) as file:
            data = json.load(file)
        return data

    def save_and_compress_data(self, data_set_name: str, data: str):
        """Compress and save data to file. Accepts data as a json string. Appending data is not possible."""
        with open(data_set_name + self.compressed_data_type, "wb") as file:
            file.write(self._compress_data(data))

    def load_and_decompress_data(self, data_set_name: str):
        """Load a compressed binary datafile and is returned as json string."""
        with open(data_set_name + self.compressed_data_type, "rb") as file:
            return self._decompress_data(file.read())

    def add_and_compress_data_stream(self, data_set_name: str, data: str, overwrite=False, close=False):
        """Compress and append data to existing file. Accepts data as json string.

            When the data stream ends, the last function call must include close=True to finalize the file
        """
        file_name = data_set_name + self.compressed_data_type

        # Check if in append mode. It not check if file exists. If so, do not overwrite and throw exception.
        if os.path.isfile(file_name) and not overwrite and not self._data_stream_append_mode:
            raise Exception(file_name + " already exists, remove the file or set overwrite flag and try again...")
        elif os.path.isfile(file_name) and overwrite:
            os.remove(file_name)
            self._data_stream_append_mode = True
        else:
            self._data_stream_append_mode = True

        # Compress data and update meta data
        compressed_data = self._compress_data_stream(data)
        self._data_stream_chunk_amount += 1
        self._data_stream_string_len.append(len(data))
        with open(file_name, "ab") as file:
            if close:
                # Update file, compress and append meta information and close file (flush). Reset meta information.
                file.write(compressed_data)
                file.write(self._compress_data_stream(self._get_current_meta_info()))
                file.write(self.compressor.flush())

                # Reset meta information and flags
                self._data_stream_append_mode = False
                self._reset_meta_information()

            else:
                # Add data to file
                file.write(compressed_data)

    def load_and_decompress_data_stream(self, data_set_name: str, buffer_size: int = 1024):
        """Decompress data stream and returns decompressed, decoded json strings one at a time"""
        # Check if in load mode, if not set load flag
        if not self._data_stream_load_mode:
            self._set_data_stream_load_mode(data_set_name)

        with open(data_set_name + self.compressed_data_type, "rb") as file:
            while True:
                # Continue in file where reader left off first time, and store new position after reading
                file.seek(self._load_index)
                bin_buffer = file.read(buffer_size)
                self._load_index = file.tell()

                # If all data chunks returned, reset load mode and return None
                if self._current_chunk_loaded > self._load_chunk_total-1:
                    self._reset_data_stream_load_mode()
                    return None

                # If binary buffer is empty, en of file has been reached and all data chunks should be loaded
                if not bin_buffer:
                    chunk_in_data = True
                else:
                    chunk_in_data = self._check_chunk_in_data(self._decompress_data_stream(bin_buffer))

                # If a the new data chunk is contained in the buffer, increment current chunk and update chunk length
                if chunk_in_data:
                    # If so extract data chunk from buffer
                    chunk_len = self._load_chunk_string_length[self._current_chunk_loaded]
                    data_chunk = self._load_data_buffer[:chunk_len]

                    # Remove data from buffer, update chunk and continue
                    self._load_data_buffer = self._load_data_buffer[chunk_len:]
                    self._current_chunk_to_load += 1
                    self._current_chunk_loaded += 1
                    return data_chunk

    def _check_chunk_in_data(self, decompressed_data):
        """Check if complete data chunk is contained in data currently loaded. Returns True if chunk in data."""
        # Feed new chunk of binary data and try to extract data chunk.
        self._string_length_loaded += decompressed_data.__len__()
        self._load_data_buffer += decompressed_data

        # Expected chunk length (total length of all chunks up to and including current on processed)
        expected_total_chunk_len = 0
        for str_len in self._load_chunk_string_length[0:self._current_chunk_to_load + 1]:
            expected_total_chunk_len += str_len

        # If total length > expected length chunk is loaded
        if self._string_length_loaded >= expected_total_chunk_len:
            return True
        return False

    def is_loading(self):
        """Return true if loading a file."""
        return self._data_stream_load_mode

    def _reset_compressor(self):
        """Reset the zlib decompressor used for compressing data streams."""
        self.compressor = self._get_data_stream_compressor()

    def _reset_decompressor(self):
        """Reset the zlib decompressor used for decompressing data streams."""
        self.decompressor = self._get_data_stream_decompressor()

    def _compress_data(self, compress_data):
        """Compress data inputted as json string for storage and returns compressed variant."""
        return zlib.compress(compress_data.encode(self.encoding_type), -1)

    def _compress_data_stream(self, compress_data):
        """Compress data as a data stream, making appending possible. Takes json strings."""
        return self.compressor.compress(compress_data.encode(self.encoding_type))

    def _decompress_data(self, decompress_data):
        """Decompress data back to original structure for use."""
        return zlib.decompress(decompress_data).decode(self.encoding_type)

    def _decompress_data_stream(self, decompress_data):
        """Decompress and decode data stream and return stored json strings."""
        data = self.decompressor.decompress(decompress_data)
        data += self.decompressor.flush()
        return data.decode(self.encoding_type)

    def _get_current_meta_info(self):
        """Return meta information of file currently processed in json format."""
        self._set_file_meta_info_dict()
        return json.dumps(self._meta_info_dict)

    def _set_file_meta_info_dict(self):
        """Set meta information of file currently processed."""
        # Check if proper keys are set or template had been edited
        keys_to_set = ["data chunk amount", "data chunk string length"]
        for key in keys_to_set:
            if key in list(self._file_meta_info_dict_template().keys()):
                self._meta_info_dict[keys_to_set[0]] = self._data_stream_chunk_amount
                self._meta_info_dict[keys_to_set[1]] = self._data_stream_string_len
            else:
                raise AttributeError("Trying to set keys not present in meta data template")

    def _reset_meta_information(self):
        """Reset meta information for current dataIO class."""
        self._data_stream_chunk_amount = 0
        self._data_stream_string_len = []
        self._set_file_meta_info_dict()

    def _get_meta_data_from_json(self, decompressed_data_string: str):
        """Retrieve the meta data dictionary stored from a json string."""
        # Load a part of the file from back to front
        for key in self._file_meta_info_dict_template().keys():
            if key not in decompressed_data_string:
                return None

        # If all tests passed extract meta data string by taking the first meta key, extract all text behind that
        start_index = decompressed_data_string.find("{\"" + list(self._file_meta_info_dict_template().keys())[0])
        return json.loads(decompressed_data_string[start_index:])  # Convert json string back to python dictionary

    def _get_meta_data_from_file(self, data_set_name: str, buffer_size: int = 1024):
        """Return meta data of a file."""
        # TODO: Do not load the whole file when decoding to speed up meta data collection
        with open(data_set_name + self.compressed_data_type, "rb") as file:
            # Get file meta data and reset file reading and decompressor
            buffer = file.read(buffer_size)
            while buffer:  # Load last part of file to see if it contains meta data
                meta_data = self._get_meta_data_from_json(self._decompress_data_stream(buffer))
                buffer = file.read(buffer_size)

                if not buffer and not meta_data:
                    # No meta data found, throw exception
                    raise Exception("No meta data found. Try to increase buffer size and try again.")
            self._reset_decompressor()
            return meta_data

    def _set_data_stream_load_mode(self, data_set_name: str):
        """Set flags and load meta data for data stream load mode."""
        self._data_stream_load_mode = True
        meta_info = self._get_meta_data_from_file(data_set_name)
        self._load_chunk_total = meta_info["data chunk amount"]
        self._load_chunk_string_length = meta_info["data chunk string length"]

    def _reset_data_stream_load_mode(self):
        """Reset flags and variables."""
        self._data_stream_load_mode = False
        self._load_index = 0
        self._load_chunk_total = 0
        self._load_chunk_string_length = []
        self._load_data_buffer = ''
        self._load_data_buffer_index = 0
        self._reset_decompressor()
        self._current_chunk_to_load = 0
        self._current_chunk_loaded = 0
        self._string_length_loaded = 0


    @staticmethod
    def _file_meta_info_dict_template():
        """Defines the template of meta information strings in files."""
        return {
            "data chunk amount": 0,
            "data chunk string length": [],
        }

    @staticmethod
    def _get_data_stream_compressor():
        """Get the default data compressor object needed to compress data streams."""
        wbits = +15
        return zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, wbits)

    @staticmethod
    def _get_data_stream_decompressor():
        """Get the default data decompressor object needed to decompress data streams."""
        wbits = +15
        return zlib.decompressobj(wbits)


if __name__ == "__main__":
    # TODO: Create testing functions for dataIO
    # TODO: Test multiple file saves and loads
    filename = "../dat/compressed_add_test"
    data_io_handler = DataIO()

    # Add data to compressed version of file in chunks
    data_io_handler.add_and_compress_data_stream(filename, "more test1, testing until you drop!", overwrite=True)
    data_io_handler.add_and_compress_data_stream(filename, "more test2")
    data_io_handler.add_and_compress_data_stream(filename, "more test3")
    data_io_handler.add_and_compress_data_stream(filename, "more test4", close=True)

    # Test loading
    bsize = 1024
    print("")
    buffer = data_io_handler.load_and_decompress_data_stream(filename, buffer_size=bsize)
    print("buffer: " + buffer.__str__())
    print("")
    while buffer:
        buffer = data_io_handler.load_and_decompress_data_stream(filename, buffer_size=bsize)
        print(buffer)
        print("")
