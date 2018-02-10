#!/usr/bin/env python

import os
import sys
import subprocess
import warnings
import nose
import h5py
import numpy

PYTOKIO_HOME = os.environ.get('PYTOKIO_HOME')
sys.path.append(os.path.join(PYTOKIO_HOME, 'tests'))
sys.path.append(os.path.join(PYTOKIO_HOME, 'tokio'))

import tokio
import tokiotest

LMTDB_SQLITE_REF = "snx11025_2018-02-09T12:00:00.sqlite3"
LMTDB_HDF5_REF = "snx11025_2018-02-09T12:00:00.hdf5"
LMTDB_START = "2018-02-09T00:00:00"
LMTDB_END = "2018-02-09T12:00:00"

H5LMT_REF = "snx11025_2018-02-09T12:00:00.h5lmt"
# the contents of the sqlite3 file only contains enough data to populate this
# many timesteps' rows; if you try to compare a generated file to H5LMT_REF
# beyond this index, you will be comparing -0.0 (generated) to real values
# (H5LMT_REF)
H5LMT_MAX_INDEX = 8640

ARCHIVE_LMTDB_BIN = os.path.join(PYTOKIO_HOME, 'bin', 'archive_lmtdb.py')

@nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)
def test_bin_archive_lmtdb():
    """Create a TOKIO HDF5 file and compare it to a reference

    Generate a new HDF5 file from a full day's worth of LMT data stored in
    SQLite and compare it to the reference file to ensure correctness.
    """
    warnings.simplefilter("always")

    tokiotest.TEMP_FILE.close()

    output_file = tokiotest.TEMP_FILE.name

    if os.path.isfile(output_file):
        os.unlink(output_file)
    cmd = [
        ARCHIVE_LMTDB_BIN,
        '--input', LMTDB_SQLITE_REF,
        '--output', output_file,
        '--init-start', LMTDB_START,
        '--init-end', LMTDB_END,
        '--timestep', '5',
        LMTDB_START,
        LMTDB_END
    ]

    print "Running [%s]" % ' '.join(cmd)
    subprocess.check_output(cmd)
    print "Created", output_file

    generated = h5py.File(output_file, 'r')

    reference = tokio.connectors.hdf5.Hdf5(H5LMT_REF, 'r')
    compare_h5lmt(generated=generated, reference=reference)

    reference = h5py.File(LMTDB_HDF5_REF, 'r')
    compare_tts(generated, reference)

def compare_h5lmt(generated, reference, tol_pct=0.005):
    """Compare a TOKIO HDF5 file to a pylmt H5LMT file

    Args:
        generated (h5py.File): newly generated HDF5 file
        reference (tokio.connectors.hdf5.Hdf5): reference H5LMT file
        tol_pct (float): percent deviation between generated and reference that
            is still considered equal (expressed as a fraction < 1.0)
    """
    dataset_names = []
    for group_name, group_data in generated.iteritems():
        for dataset_name, dataset in group_data.iteritems():
            if isinstance(dataset, h5py.Dataset):
                dataset_names.append("/%s/%s" % (group_name, dataset_name))

    checked_ct = 0
    for dataset_name in dataset_names:
        print "Comparing %s from %s" % (dataset_name, reference.filename)
        ref_dataset = reference.get(dataset_name)
        if ref_dataset is None:
            errmsg = "Cannot compare dataset %s (not in reference)" % dataset_name
            warnings.warn(errmsg)
            continue

        gen_dataset = generated.get(dataset_name)
        gen_shape = gen_dataset.shape

        if len(gen_shape) == 1:
            sum1 = gen_dataset[:].sum()
            sum2 = ref_dataset[:-1].sum()
            print "%f == %f? %s" % (sum1, sum2, numpy.isclose(sum1, sum2))
            assert numpy.isclose(sum1[:], sum2[:])
            assert sum1 > 0
            assert (numpy.isclose(gen_dataset[:], ref_dataset[:-1])).all()

        elif len(gen_shape) == 2:
            # H5LMT is semantically inconsistent across its own datasets!
            if 'OSTBulk' in ref_dataset.name:
                ref_slice = (slice(1, H5LMT_MAX_INDEX + 1), slice(None, None))
                fudged = True
            else:
                ref_slice = (slice(None, H5LMT_MAX_INDEX), slice(None, None))
                fudged = False

            match_matrix = numpy.isclose(gen_dataset[:H5LMT_MAX_INDEX, :], ref_dataset[ref_slice])
            nmatch = match_matrix.sum()
            nelements = match_matrix.shape[0] * match_matrix.shape[1]

            sum1 = gen_dataset[:H5LMT_MAX_INDEX, :].sum()
            sum2 = ref_dataset[ref_slice].sum()

            if fudged:
                pct_diff = abs(sum1 - sum2) / sum2
                print "%e < %e? %s" % (pct_diff, tol_pct, pct_diff < tol_pct)
                assert pct_diff < tol_pct

                pct_diff = (1.0 - float(nmatch) / float(nelements))
                assert pct_diff < tol_pct
            else:
                print "%f == %f? %s" % (sum1, sum2, numpy.isclose(sum1, sum2))
                assert numpy.isclose(sum1, sum2)
                assert nmatch == nelements

            assert sum1 > 0
        checked_ct += 1

    print "Verified %d datasets against reference" % checked_ct
    assert checked_ct > 0

def compare_tts(generated, reference):
    """Compare two TOKIO HDF5 files

    Args:
        generated (h5py.File): newly generated HDF5 file
        reference (h5py.File): reference HDF5 file
    """
    checked_ct = 0
    for group_name, group_data in generated.iteritems():
        if group_name not in reference:
            errmsg = "Cannot compare group %s (does not exist in reference)" % group_name
            warnings.warn(errmsg)
            continue
        ref_group = reference[group_name]
        for dataset_name, dataset in group_data.iteritems():
            if not isinstance(dataset, h5py.Dataset):
                continue
            ref_dataset = ref_group[dataset_name]
            print "Comparing", dataset_name
            checked_ct += 1
            if dataset_name not in ref_group:
                errmsg = "Cannot compare dataset %s (not in reference)" % dataset_name
                warnings.warn(errmsg)
                continue
            assert dataset_name in ref_group

            shape = ref_dataset.shape
            assert shape == dataset.shape

            if len(shape) == 1:
                assert (numpy.isclose(dataset[:], ref_dataset[:])).all()
                sum1 = dataset[:].sum()
                sum2 = ref_dataset[:].sum()
                print "%f == %f? %s" % (sum1, sum2, numpy.isclose(sum1, sum2))
                assert numpy.isclose(sum1, sum2)
                assert sum1 > 0
            elif len(shape) == 2:
                assert (numpy.isclose(dataset[:, :], ref_dataset[:, :])).all()
                sum1 = dataset[:].sum().sum()
                sum2 = ref_dataset[:].sum().sum()
                print "%f == %f? %s" % (sum1, sum2, numpy.isclose(sum1, sum2))
                assert numpy.isclose(sum1, sum2)
                assert sum1 > 0
    assert checked_ct > 0
