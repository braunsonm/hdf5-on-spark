import argparse
import os
import glob
import datetime
from timeit import default_timer as timer

import h5py


class Filter:
    def __init__(self):
        self.count = 0
        self.total = 0

    def parse_timestamp(self, timestamp):
        """
        These files have such a mess of timestamp handling.
        :param timestamp:
        :return:
        """
        formatted = timestamp
        if "'T'" in timestamp or ('T' in timestamp and 'CST' not in timestamp and 'UTC' not in timestamp):
            if '.' in timestamp and "'" in timestamp:
                formatted = datetime.datetime.strptime(timestamp, "%Y-%m-%d'T'%H:%M:%S.%f").timestamp()
            elif '.' in timestamp:
                formatted = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f").timestamp()
            elif "'" in timestamp:
                formatted = datetime.datetime.strptime(timestamp, "%Y-%m-%d'T'%H:%M:%S").timestamp()
            else:
                formatted = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S").timestamp()
        elif ' ' in timestamp:
            if '-' in timestamp:
                formatted = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f %Z%z").timestamp()
            elif '.' in timestamp:
                formatted = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S.%f").timestamp()
            else:
                formatted = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").timestamp()
        else:
            formatted = float(timestamp)

        return round(formatted*1000)

    def filter(self, file, **kwargs):
        for dataset in file['data'].values():
            attributes = dataset.attrs
            cond = True
            if kwargs['time_gte'] is not None:
                timestamp = self.parse_timestamp(attributes['timestamp'].decode('utf-8'))
                cond = cond and kwargs['time_gte'] <= timestamp <= kwargs['time_lte']

            if kwargs['latitude_gte'] is not None:
                latitude = float(attributes['Latitude'].decode('utf-8'))
                cond = cond and kwargs['latitude_gte'] <= latitude <= kwargs['latitude_lte']

            if kwargs['longitude_gte'] is not None:
                longitude = float(attributes['Longitude'].decode('utf-8'))
                cond = cond and kwargs['longitude_gte'] <= longitude <= kwargs['longitude_lte']

            # In order to really say it was "ready for processing" we need to read in the dataset
            # Note: Read future work for an optimization to the Spark implementation that does not read in the whole
            # payload before checking the metadata.
            readindata = dataset[0:]
            if cond:
                self.count += 1
            self.total += 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', metavar='directory', help="The directory containing HDF5 files to read. Please use absolute paths.")
    parser.add_argument('--time-gte', dest='time_gte', type=int)
    parser.add_argument('--time-lte', dest='time_lte', type=int)
    parser.add_argument('--latitude-gte', dest='latitude_gte', type=float)
    parser.add_argument('--latitude-lte', dest='latitude_lte', type=float)
    parser.add_argument('--longitude-gte', dest='longitude_gte', type=float)
    parser.add_argument('--longitude-lte', dest='longitude_lte', type=float)
    args = parser.parse_args()

    file = None
    if os.path.isabs(args.directory):
        path = args.directory
    else:
        raise ValueError("File path must be absolute")

    if (args.time_gte is None) != (args.time_lte is None):
        raise ValueError("If one of time filters is specified, then both must be")
    if (args.latitude_gte is None) != (args.latitude_lte is None):
        raise ValueError("If one of latitude filters is specified, then both must be")
    if (args.longitude_gte is None) != (args.longitude_lte is None):
        raise ValueError("If one of longitude filters is specified, then both must be")

    filter = Filter()
    time = timer()
    for files in glob.glob(path + os.path.sep + "*.hdf5"):
        print(f"Inspecting {files}")
        file = h5py.File(files, 'r')
        filter.filter(file, **vars(args))
        file.close()

    time = timer() - time
    print(f"Filtered out {filter.count} of {filter.total} items, took {time}s")
