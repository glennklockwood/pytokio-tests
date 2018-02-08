pytokio-tests
================================================================================

This repository contains some larger functionality and correctness tests for
[pytokio][] that require datasets that are too large to include in the pytokio
repository itself.  Someday this repository may be merged back into pytokio,
but for now, it should be checked out separately.

To run tests, export the `PYTOKIO_HOME` directory to point at your pytokio
repository before running nosetests.

    PYTOKIO_HOME=$(readlink -f ../pytokio-dev) nosetests -v

License
--------------------------------------------------------------------------------

Total Knowledge of I/O Copyright (c) 2017, The Regents of the University of
California, through Lawrence Berkeley National Laboratory (subject to receipt
of any required approvals from the U.S. Dept. of Energy).  All rights reserved.

If you have questions about your rights to use or distribute this software,
please contact Berkeley Lab's Innovation & Partnerships Office at IPO@lbl.gov.

NOTICE.  This Software was developed under funding from the U.S. Department of
Energy and the U.S. Government consequently retains certain rights. As such,
the U.S. Government has been granted for itself and others acting on its behalf
a paid-up, nonexclusive, irrevocable, worldwide license in the Software to
reproduce, distribute copies to the public, prepare derivative works, and
perform publicly and display publicly, and to permit other to do so.

[pytokio]: https://github.com/nersc/pytokio/
