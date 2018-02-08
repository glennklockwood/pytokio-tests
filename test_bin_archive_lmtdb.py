#!/usr/bin/env python

import os
import sys
import subprocess
import nose
import h5py
import numpy

PYTOKIO_HOME = os.environ.get('PYTOKIO_HOME')
sys.path.append(os.path.join(PYTOKIO_HOME, 'tests'))
sys.path.append(os.path.join(PYTOKIO_HOME, 'tokio'))

import tokio
import tokiotest

LMTDB_SQLITE_REF = "output_snx11025_2018-01-30.sqlite3"
LMTDB_HDF5_REF = "output_snx11025_2018-01-30.hdf5"
LMTDB_START = "2018-01-30T00:00:00"
LMTDB_END = "2018-01-31T00:00:00"

ARCHIVE_LMTDB_BIN = os.path.join(PYTOKIO_HOME, 'bin', 'archive_lmtdb.py')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_lmtdb():
    """
    Generate a new HDF5 file from a full day's worth of LMT data stored in
    SQLite and compare it to the reference file to ensure correctness
    """

    tokiotest.TEMP_FILE.close()

    reference = h5py.File(LMTDB_HDF5_REF, 'r')

    output_file = tokiotest.TEMP_FILE.name

    if os.path.isfile(output_file):
        os.unlink(output_file)
    cmd = [
        ARCHIVE_LMTDB_BIN,
        '--input',
        LMTDB_SQLITE_REF,
        '--output',
        output_file,
        '--init-start',
        LMTDB_START,
        '--init-end',
        LMTDB_END,
        '--timestep',
        '5',
        LMTDB_START,
        LMTDB_END
    ]

    print "Running [%s]" % ' '.join(cmd)
    subprocess.check_output(cmd)
    print "Created", output_file

    # We're relying on the specific two-level hierarchy of the TOKIO HDF5 format
    checked_ct = 0
    generated = h5py.File(output_file, 'r')
    for group_name, group_data in generated.iteritems():
        for dataset_name, dataset in group_data.iteritems():
            if isinstance(dataset, h5py.Dataset):
                print "Comparing", dataset_name
                checked_ct += 1
                assert dataset_name in reference[group_name].keys()

                shape = reference[group_name][dataset_name].shape
                assert shape == dataset.shape

                if len(shape) == 1:
                    assert (numpy.isclose(dataset[:], reference[group_name][dataset_name][:])).all()
                elif len(shape) == 2:
                    assert (numpy.isclose(dataset[:, :], reference[group_name][dataset_name][:, :])).all()

    assert checked_ct > 0
