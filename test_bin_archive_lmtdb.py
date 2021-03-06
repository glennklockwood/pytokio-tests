#!/usr/bin/env python
"""Expanded correctness tests for archive_lmtdb.py

Generates a TOKIO HDF5 file using archive_lmtdb.py and compares its contents to
reference datasets that are known to contain true values.  Checks for numerical
equality/closeness where appropriate.
"""

import os
import sys
import subprocess
import warnings
import tempfile
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
TOLERANCE_PCT = 0.005 # expressed as fraction, not percent

ARCHIVE_LMTDB_BIN = os.path.join(PYTOKIO_HOME, 'bin', 'archive_lmtdb.py')

class TestArchiveLmtdbCorrectness(object):
# @nose.tools.with_setup(tokiotest.create_tempfile, tokiotest.delete_tempfile)

    @classmethod
    def setup_class(self):
        """Compare TOKIO HDF5 to reference files

        Generate a new HDF5 file from a full day's worth of LMT data stored in
        SQLite and compare it to the reference file to ensure correctness.
        """
        with tempfile.NamedTemporaryFile(delete=True) as file_obj:
            self.output_file = file_obj.name

        warnings.simplefilter("always")

        if os.path.isfile(self.output_file):
            os.unlink(self.output_file)
        cmd = [
            ARCHIVE_LMTDB_BIN,
            '--input', LMTDB_SQLITE_REF,
            '--output', self.output_file,
            '--init-start', LMTDB_START,
            '--init-end', LMTDB_END,
            '--timestep', '5',
            LMTDB_START,
            LMTDB_END
        ]

        print "Running [%s]" % ' '.join(cmd)
        subprocess.check_output(cmd)
        print "Created", self.output_file

        self.generated = h5py.File(self.output_file, 'r')

        self.generated_tts = tokio.connectors.hdf5.Hdf5(self.output_file, 'r')

        self.ref_h5lmt = tokio.connectors.hdf5.Hdf5(H5LMT_REF, 'r')

        self.ref_hdf5 = h5py.File(LMTDB_HDF5_REF, 'r')

    @classmethod
    def teardown_class(self):
        self.generated.close()
        os.unlink(self.output_file)
        self.ref_hdf5.close()
        self.ref_h5lmt.close()

    def test_compare_h5lmt(self):
        """Compare a TOKIO HDF5 file to a pylmt H5LMT file

        Args:
            generated (h5py.File): newly generated HDF5 file
            reference (tokio.connectors.hdf5.Hdf5): reference H5LMT file
        """
        dataset_names = []
        for group_name, group_data in self.generated.iteritems():
            for dataset_name, dataset in group_data.iteritems():
                if isinstance(dataset, h5py.Dataset):
                    dataset_names.append("/%s/%s" % (group_name, dataset_name))

        checked_ct = 0
        for dataset_name in dataset_names:
            if dataset_name.endswith('timestamps'):
                # don't bother with timestamps datasets directly; we handle them
                # when we encounter the parent dataset
                continue

            # check the dataset
            func = compare_h5lmt_dataset
            func.description = "comparing %s dataset inside h5lmt" % dataset_name
            print func.description
            yield func, self.generated, self.ref_h5lmt, dataset_name, False

            # check the timestamp
            func.description = "comparing %s's timestamps dataset inside h5lmt" % dataset_name
            print func.description
            yield func, self.generated_tts, self.ref_h5lmt, dataset_name, True
            checked_ct += 1

        print "Verified %d datasets against reference" % checked_ct
        assert checked_ct > 0

    def test_compare_tts(self):
        """Compare two TOKIO HDF5 files
        """
        checked_ct = 0
        for group_name, group_data in self.generated.iteritems():
            for dataset in group_data.itervalues():
                if not isinstance(dataset, h5py.Dataset):
                    continue
                dataset_name = dataset.name

                func = compare_tts
                func.description = "comparing %s dataset inside reference hdf5" % dataset_name
                print func.description
                yield func, self.generated, self.ref_hdf5, dataset_name
                checked_ct += 1

        assert checked_ct > 0

def compare_tts(generated, reference, dataset_name):
    """Compare a single dataset between a generated HDF5 and a reference HDF5
    """
    print "Comparing %s in %s to %s" % (dataset_name, generated.filename, reference.filename)
    ref_dataset = reference.get(dataset_name)
    gen_dataset = generated.get(dataset_name)
    if ref_dataset is None:
        errmsg = "Cannot compare dataset %s (not in reference)" % dataset_name
        nose.SkipTest(errmsg)
        return
    elif gen_dataset is None:
        errmsg = "Cannot compare dataset %s (not in generated)" % dataset_name
        nose.SkipTest(errmsg)
        return

    print "Checking shape: generated %s == reference %s? %s" % (gen_dataset.shape, ref_dataset.shape, gen_dataset.shape == ref_dataset.shape)
    assert gen_dataset.shape == ref_dataset.shape

    if len(gen_dataset.shape) == 1:
        assert (numpy.isclose(gen_dataset[:], ref_dataset[:])).all()
        sum_gen = gen_dataset[:].sum()
        sum_ref = ref_dataset[:].sum()
        print "generated %f == reference %f? %s" % (sum_gen, sum_ref, numpy.isclose(sum_gen, sum_ref))
        assert numpy.isclose(sum_gen, sum_ref)
        assert sum_gen > 0
    elif len(gen_dataset.shape) == 2:
        assert (numpy.isclose(gen_dataset[:, :], ref_dataset[:, :])).all()
        sum_gen = gen_dataset[:].sum().sum()
        sum_ref = ref_dataset[:].sum().sum()
        print "%f == %f? %s" % (sum_gen, sum_ref, numpy.isclose(sum_gen, sum_ref))
        assert numpy.isclose(sum_gen, sum_ref)
        assert sum_gen > 0

def compare_h5lmt_dataset(generated, ref_h5lmt, dataset_name, check_timestamps=False):
    """Compare a single dataset between an H5LMT and TOKIO HDF5 file
    """
    print "Comparing %s in %s to %s" % (dataset_name, generated.filename, ref_h5lmt.filename)

    ref_dataset = ref_h5lmt.get(dataset_name)
    gen_dataset = generated.get(dataset_name)
    if ref_dataset is None:
        errmsg = "Cannot compare dataset %s (not in reference)" % dataset_name
        nose.SkipTest(errmsg)
        return
    elif gen_dataset is None:
        errmsg = "Cannot compare dataset %s (not in generated)" % dataset_name
        nose.SkipTest(errmsg)
        return

    if check_timestamps:
        ref_dataset = ref_h5lmt.get_timestamps(dataset_name)
        gen_dataset = generated.get_timestamps(dataset_name)

    gen_shape = gen_dataset.shape

    if len(gen_shape) == 1 or check_timestamps:
        sum_gen = gen_dataset[:H5LMT_MAX_INDEX].sum()
        sum_ref = ref_dataset[:H5LMT_MAX_INDEX].sum()
        print "%f == %f? %s" % (sum_gen, sum_ref, numpy.isclose(sum_gen, sum_ref))
        assert numpy.isclose(sum_gen, sum_ref)
        assert sum_gen > 0
        assert (numpy.isclose(gen_dataset[:H5LMT_MAX_INDEX], ref_dataset[:H5LMT_MAX_INDEX])).all()

    elif len(gen_shape) == 2:
        # H5LMT is semantically inconsistent across its own datasets!
        if 'OSTBulk' in ref_dataset.name or 'MDSOpsDataSet' in ref_dataset.name:
            ref_slice = (slice(1, H5LMT_MAX_INDEX + 1), slice(None, None))
            gen_slice = (slice(None, H5LMT_MAX_INDEX), slice(None, None))
            fudged = True
            print "%s is fudged; doing imperfect comparisons" % ref_dataset.name
        else:
            ref_slice = (slice(None, H5LMT_MAX_INDEX), slice(None, None))
            gen_slice = (slice(None, H5LMT_MAX_INDEX), slice(None, None))
            fudged = False
            print "%s is not fudged; doing exact comparisons" % ref_dataset.name

        print "Comparing gen(%s, %s) to ref(%s, %s)" % (gen_dataset.name, gen_dataset[gen_slice].shape, ref_dataset.name, ref_dataset[ref_slice].shape)
        print "gen(%s) ref(%s)" % (type(gen_dataset), type(ref_dataset))
        match_matrix = numpy.isclose(gen_dataset[gen_slice], ref_dataset[ref_slice])
        nmatch = match_matrix.sum()
        nelements = match_matrix.shape[0] * match_matrix.shape[1]

        sum_gen = gen_dataset[gen_slice].sum()
        sum_ref = ref_dataset[ref_slice].sum()

        if fudged:
            tol = TOLERANCE_PCT
        else:
            tol = TOLERANCE_PCT / 10.0

        # how big is the skew as a percent of the true value?
        pct_diff = abs(sum_gen - sum_ref) / sum_ref
        print "pct_diff %e < tolerance %e? %s" % (pct_diff, tol, pct_diff < tol)
        assert pct_diff < tol 

        # what fraction of elements match?
        pct_diff = (1.0 - float(nmatch) / float(nelements))
        assert pct_diff < tol

# isclose doesn't work in the presence of missing data for OSSCPUDataSet
#       print "generated %f == reference %f? %s" % (sum_gen, sum_ref, numpy.isclose(sum_gen, sum_ref))
#       isclose = numpy.isclose(sum_gen, sum_ref)
        # h5lmt includes interpolated values rather than mark them missing;
        # mask those off
#       missing_data = tokio.timeseries.negative_zero_matrix(gen_dataset[gen_slice]) > 0.5
#       assert (isclose | missing_data).all()
#       assert nmatch == nelements

        assert sum_gen > 0
